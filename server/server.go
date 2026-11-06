// Package server implements a replicated ZooKeeper server as described in
// "ZooKeeper: Wait-free coordination for Internet-scale systems".
//
// Each server holds a full in-memory replica of the data tree. Reads are
// served locally; writes are forwarded to the leader, which turns them into
// idempotent transactions and broadcasts them with a Zab-style atomic
// broadcast protocol (leader election, discovery, synchronization and
// broadcast phases). Transactions are written to a write-ahead log before
// they are acknowledged, and fuzzy snapshots bound recovery time.
package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"zookeeper/transport"
	"zookeeper/zk"
)

// Config configures a server.
type Config struct {
	// ID identifies the server in the ensemble; it must be a key of Peers.
	ID int64
	// Peers maps every server id of the ensemble to its address. Clients,
	// followers and election traffic all use this address.
	Peers map[int64]string
	// Transport defaults to TCP.
	Transport transport.Transport
	// Storage defaults to an in-memory storage.
	Storage Storage
	// TickTime is the basic time unit (default 100ms).
	TickTime time.Duration
	// InitLimit is the number of ticks allowed for followers to connect and
	// synchronize with a leader (default 20).
	InitLimit int
	// SyncLimit is the number of ticks a follower may lag before the leader
	// considers it gone (default 5). Followers abandon a silent leader after
	// twice that, so leaders step down first (Section 4.4, sync).
	SyncLimit int
	// SnapCount is the number of transactions between snapshots
	// (default 10000, negative disables snapshots).
	SnapCount int
	// Session timeout bounds (defaults 2 ticks and 60s).
	MinSessionTimeout time.Duration
	MaxSessionTimeout time.Duration
	// Logger receives debug output; nil disables logging.
	Logger *log.Logger
}

// Server is one member of a ZooKeeper ensemble.
type Server struct {
	cfg    Config
	id     int64
	quorum int
	ln     transport.Listener
	quit   chan struct{}
	wg     sync.WaitGroup

	// mu protects the Zab state: epochs, the log, leader/follower state.
	// Transactions are applied with mu held, so commits are totally ordered.
	mu            sync.Mutex
	cond          *sync.Cond
	state         PeerState
	stopping      bool
	acceptedEpoch int64
	currentEpoch  int64
	log           []*Txn // proposals logged, sorted by zxid
	logBase       int64  // every txn after logBase is in log
	lastLogged    int64
	lastCommitted int64
	leaderID      int64
	leader        *leaderState
	follower      transport.Conn // connection to the leader while following
	round         int64
	txnsSinceSnap int
	snapRunning   bool
	snapGen       int64
	prevSnapZxid  int64

	electionIn chan *Notification

	// dbMu protects the replicated database.
	dbMu    sync.RWMutex
	tree    *DataTree
	watches *watchManager

	smu      sync.Mutex
	sessions map[int64]*clientSession // sessions connected here
	connects map[int64]chan zk.Code   // pending session creations

	touchMu sync.Mutex
	touched map[int64]struct{}

	connMu sync.Mutex
	conns  map[transport.Conn]struct{}

	peerMu  sync.Mutex
	peerOut map[int64]*peerSender

	serving atomic.Bool
}

// ErrNotServing is returned when a write cannot be routed to a leader.
var ErrNotServing = errors.New("server: not serving")

// New creates a server and recovers its state from storage.
func New(cfg Config) (*Server, error) {
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("server: id %d is not in the peer list", cfg.ID)
	}
	if cfg.Transport == nil {
		cfg.Transport = transport.TCP{}
	}
	if cfg.Storage == nil {
		cfg.Storage = NewMemStorage()
	}
	if cfg.TickTime <= 0 {
		cfg.TickTime = 100 * time.Millisecond
	}
	if cfg.InitLimit <= 0 {
		cfg.InitLimit = 20
	}
	if cfg.SyncLimit <= 0 {
		cfg.SyncLimit = 5
	}
	if cfg.SnapCount == 0 {
		cfg.SnapCount = 10000
	}
	if cfg.MinSessionTimeout <= 0 {
		cfg.MinSessionTimeout = 2 * cfg.TickTime
	}
	if cfg.MaxSessionTimeout <= 0 {
		cfg.MaxSessionTimeout = 60 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(io.Discard, "", 0)
	}
	s := &Server{
		cfg:        cfg,
		id:         cfg.ID,
		quorum:     len(cfg.Peers)/2 + 1,
		quit:       make(chan struct{}),
		electionIn: make(chan *Notification, 1024),
		watches:    newWatchManager(),
		sessions:   map[int64]*clientSession{},
		connects:   map[int64]chan zk.Code{},
		touched:    map[int64]struct{}{},
		conns:      map[transport.Conn]struct{}{},
		peerOut:    map[int64]*peerSender{},
	}
	s.cond = sync.NewCond(&s.mu)
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

// recover loads the latest snapshot and the log. Logged transactions after
// the snapshot are not applied yet: they are committed when the server
// synchronizes with the leader (or becomes the leader).
func (s *Server) recover() error {
	st := s.cfg.Storage
	snap, err := st.LoadSnapshot()
	if err != nil {
		return err
	}
	if snap != nil {
		s.tree = TreeFromSnapshot(snap)
		s.lastCommitted = snap.Zxid
		s.prevSnapZxid = snap.Zxid
	} else {
		s.tree = NewDataTree()
	}
	s.logBase = s.lastCommitted
	s.log, err = st.Entries()
	if err != nil {
		return err
	}
	s.lastLogged = s.lastCommitted
	if n := len(s.log); n > 0 && s.log[n-1].Zxid > s.lastLogged {
		s.lastLogged = s.log[n-1].Zxid
	}
	s.acceptedEpoch, s.currentEpoch, err = st.LoadEpochs()
	return err
}

// Start begins listening and joins the ensemble.
func (s *Server) Start() error {
	ln, err := s.cfg.Transport.Listen(s.cfg.Peers[s.id])
	if err != nil {
		return err
	}
	s.ln = ln
	s.wg.Add(2)
	go s.acceptLoop()
	go s.run()
	return nil
}

// Close stops the server. Storage is left open so that a new server can be
// started on it (simulating a restart); call Storage.Close separately.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return nil
	}
	s.stopping = true
	s.state = Stopped
	s.cond.Broadcast()
	s.mu.Unlock()
	close(s.quit)
	s.serving.Store(false)
	if s.ln != nil {
		s.ln.Close()
	}
	s.connMu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.connMu.Unlock()
	s.closeAllClients()
	s.wg.Wait()
	return nil
}

func (s *Server) logf(format string, args ...any) {
	s.cfg.Logger.Printf("[server %d] "+format, append([]any{s.id}, args...)...)
}

func (s *Server) trackConn(c transport.Conn) bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	select {
	case <-s.quit:
		c.Close()
		return false
	default:
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrackConn(c transport.Conn) {
	s.connMu.Lock()
	delete(s.conns, c)
	s.connMu.Unlock()
	c.Close()
}

// ---- accessors ----

// ID returns the server id.
func (s *Server) ID() int64 { return s.id }

// Addr returns the listening address.
func (s *Server) Addr() string { return s.cfg.Peers[s.id] }

// State returns the current role.
func (s *Server) State() PeerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// LeaderID returns the id of the current leader (0 while looking).
func (s *Server) LeaderID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == Looking {
		return 0
	}
	return s.leaderID
}

// Serving reports whether the server accepts client sessions.
func (s *Server) Serving() bool { return s.serving.Load() }

// LastZxid returns the zxid of the last transaction applied locally.
func (s *Server) LastZxid() int64 {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.tree.lastZxid
}

// Digest returns a hash of the local replica (see DataTree.Digest).
func (s *Server) Digest() [32]byte {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.tree.Digest()
}

// NodeCount returns the number of znodes in the local replica.
func (s *Server) NodeCount() int {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.tree.NodeCount()
}

// ---- main loop ----

func (s *Server) run() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return
		}
		s.state = Looking
		s.mu.Unlock()

		leader, ok := s.lookForLeader()
		if !ok {
			return
		}
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return
		}
		s.leaderID = leader
		if leader == s.id {
			s.state = Leading
		} else {
			s.state = Following
		}
		s.mu.Unlock()
		s.logf("elected leader %d", leader)
		if leader == s.id {
			s.lead()
		} else {
			s.follow(leader)
		}
		s.serving.Store(false)
		s.closeAllClients()
	}
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		if !s.trackConn(c) {
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrackConn(c)
			first, err := c.Recv()
			if err != nil {
				return
			}
			switch m := first.(type) {
			case *ElectionHello:
				s.readElection(c)
			case *FollowerInfo:
				s.serveLearner(c, m)
			case *zk.ConnectRequest:
				s.handleClient(c, m)
			}
		}()
	}
}

// ---- write path entry points ----

// submitWrite routes a client write to the leader (directly if we lead).
func (s *Server) submitWrite(f *Forward) error {
	s.mu.Lock()
	if s.state == Leading && s.leader != nil && s.leader.established {
		s.leaderProcess(f)
		s.mu.Unlock()
		return nil
	}
	c := s.follower
	s.mu.Unlock()
	if c == nil {
		return ErrNotServing
	}
	return c.Send(f)
}

// submitSync implements sync: the response is produced after every commit
// that was pending at the leader when the sync reached it (Section 4.4).
func (s *Server) submitSync(sessionID, xid int64) error {
	s.mu.Lock()
	if s.state == Leading && s.leader != nil && s.leader.established {
		s.leaderSync(nil, sessionID, xid)
		s.mu.Unlock()
		return nil
	}
	c := s.follower
	s.mu.Unlock()
	if c == nil {
		return ErrNotServing
	}
	return c.Send(&SyncRequest{SessionID: sessionID, Xid: xid})
}

// ---- log and apply (called with s.mu held) ----

func (s *Server) appendLog(txns ...*Txn) error {
	if len(txns) == 0 {
		return nil
	}
	if err := s.cfg.Storage.Append(txns); err != nil {
		return err
	}
	s.log = append(s.log, txns...)
	if z := txns[len(txns)-1].Zxid; z > s.lastLogged {
		s.lastLogged = z
	}
	return nil
}

// logIndexAfter returns the index of the first entry with zxid > z.
func (s *Server) logIndexAfter(z int64) int {
	return sort.Search(len(s.log), func(i int) bool { return s.log[i].Zxid > z })
}

func (s *Server) logContains(z int64) bool {
	i := sort.Search(len(s.log), func(i int) bool { return s.log[i].Zxid >= z })
	return i < len(s.log) && s.log[i].Zxid == z
}

// applyCommitted applies every logged transaction up to zxid z.
func (s *Server) applyCommitted(z int64) {
	if z <= s.lastCommitted {
		return
	}
	for i := s.logIndexAfter(s.lastCommitted); i < len(s.log) && s.log[i].Zxid <= z; i++ {
		s.applyTxn(s.log[i])
	}
	s.lastCommitted = z
}

// applyTxn applies a committed transaction to the database, fires watches
// and answers the client if the request was received by this server.
func (s *Server) applyTxn(txn *Txn) {
	s.dbMu.Lock()
	res := s.tree.Apply(txn)
	for _, t := range res.triggers {
		s.watches.trigger(t.path, t.typ, txn.Zxid)
	}
	s.dbMu.Unlock()

	if s.leader != nil {
		s.leader.applied(txn)
	}

	local := txn.ServerID == s.id
	switch txn.Type {
	case TxnCreateSession:
		if local {
			s.smu.Lock()
			ch := s.connects[txn.SessionID]
			s.smu.Unlock()
			if ch != nil {
				ch <- zk.CodeOK
			}
		}
	case TxnCloseSession:
		if cs := s.localSession(txn.SessionID); cs != nil {
			if local && txn.Xid != 0 {
				cs.complete(txn.Xid, &zk.Response{Xid: txn.Xid, Zxid: txn.Zxid})
			}
			s.unregisterSession(cs)
			cs.close()
		}
	default:
		if local {
			resp := &zk.Response{Xid: txn.Xid, Zxid: txn.Zxid, Err: res.err}
			switch txn.Type {
			case TxnCreate:
				resp.Path = txn.Path
			case TxnSetData:
				st := res.stat
				resp.Stat = &st
			case TxnError:
				resp.Err = txn.Err
			}
			s.completeLocal(txn.SessionID, txn.Xid, resp)
		}
	}

	s.txnsSinceSnap++
	if s.cfg.SnapCount > 0 && s.txnsSinceSnap >= s.cfg.SnapCount && !s.snapRunning {
		s.txnsSinceSnap = 0
		s.snapRunning = true
		gen := s.snapGen
		s.wg.Add(1)
		go s.takeSnapshot(gen)
	}
}

// takeSnapshot writes a fuzzy snapshot (Section 4.3): the tree is walked
// depth-first without blocking writers, locking only while each znode is
// read. The result may not correspond to any real state, but replaying the
// log from the zxid recorded at the start fixes it because transactions are
// idempotent.
func (s *Server) takeSnapshot(gen int64) {
	defer s.wg.Done()
	snap := s.fuzzySnapshot()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapRunning = false
	if gen != s.snapGen || s.stopping {
		return // state was replaced by a SNAP from the leader meanwhile
	}
	if err := s.cfg.Storage.SaveSnapshot(snap); err != nil {
		s.logf("snapshot failed: %v", err)
		return
	}
	// Keep one snapshot interval of log so lagging followers can still be
	// synchronized with a diff.
	if s.prevSnapZxid > s.logBase {
		if err := s.cfg.Storage.Compact(s.prevSnapZxid); err == nil {
			s.log = append([]*Txn(nil), s.log[s.logIndexAfter(s.prevSnapZxid):]...)
			s.logBase = s.prevSnapZxid
		}
	}
	s.prevSnapZxid = snap.Zxid
	s.logf("snapshot at zxid %x (%d nodes)", snap.Zxid, len(snap.Nodes))
}

func (s *Server) fuzzySnapshot() *Snapshot {
	s.dbMu.RLock()
	snap := &Snapshot{Zxid: s.tree.lastZxid, Sessions: s.tree.Sessions()}
	s.dbMu.RUnlock()
	stack := []string{"/"}
	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		s.dbMu.RLock()
		n := s.tree.nodes[p]
		if n == nil {
			s.dbMu.RUnlock()
			continue
		}
		sn := SnapNode{Path: p, Data: n.data, Stat: n.stat}
		kids, _, _ := s.tree.Children(p)
		s.dbMu.RUnlock()
		snap.Nodes = append(snap.Nodes, sn)
		for i := len(kids) - 1; i >= 0; i-- {
			stack = append(stack, zk.Join(p, kids[i]))
		}
	}
	return snap
}

// installSnapshot replaces the local state with the leader's (SNAP sync).
func (s *Server) installSnapshot(snap *Snapshot, entries []*Txn) error {
	s.snapGen++
	if err := s.cfg.Storage.SaveSnapshot(snap); err != nil {
		return err
	}
	if err := s.cfg.Storage.Reset(entries); err != nil {
		return err
	}
	s.dbMu.Lock()
	s.tree = TreeFromSnapshot(snap)
	s.dbMu.Unlock()
	s.watches.clear()
	s.log = append([]*Txn(nil), entries...)
	s.logBase = snap.Zxid
	s.prevSnapZxid = snap.Zxid
	s.lastCommitted = snap.Zxid
	s.lastLogged = snap.Zxid
	if n := len(entries); n > 0 {
		s.lastLogged = entries[n-1].Zxid
	}
	return nil
}

// waitCond waits (with s.mu held) until pred holds, the timeout expires or
// the server stops.
func (s *Server) waitCond(d time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(d)
	t := time.AfterFunc(d, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer t.Stop()
	for !pred() {
		if s.stopping || !time.Now().Before(deadline) {
			return false
		}
		s.cond.Wait()
	}
	return true
}

func (s *Server) initTimeout() time.Duration {
	return s.cfg.TickTime * time.Duration(s.cfg.InitLimit)
}

func (s *Server) syncTimeout() time.Duration {
	return s.cfg.TickTime * time.Duration(s.cfg.SyncLimit)
}

// recvTimeout reads from a channel fed by a connection reader.
func recvTimeout(ch <-chan any, d time.Duration, quit <-chan struct{}) (any, bool) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case m, ok := <-ch:
		return m, ok
	case <-t.C:
		return nil, false
	case <-quit:
		return nil, false
	}
}

func epochOf(zxid int64) int64   { return zxid >> 32 }
func counterOf(zxid int64) int64 { return zxid & 0xffffffff }
func makeZxid(epoch, counter int64) int64 {
	return epoch<<32 | (counter & 0xffffffff)
}
