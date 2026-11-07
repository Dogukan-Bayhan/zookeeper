package server

import (
	"fmt"
	"time"

	"zookeeper/transport"
	"zookeeper/zk"
	"zookeeper/zlog"
)

// leaderState is the state of a server while it leads the ensemble. All
// fields are protected by Server.mu.
type leaderState struct {
	s       *Server
	epoch   int64
	counter int64

	learners   map[int64]*learnerHandler // connected followers
	forwarding map[int64]*learnerHandler // synchronized followers receiving proposals
	pending    []*proposal               // outstanding proposals in zxid order

	records     map[string]*changeRecord // future state of outstanding changes
	tracker     map[int64]*trackedSession
	syncWaiters map[int64][]syncWaiter

	// discovery / synchronization
	infos            map[int64]int64 // follower id -> accepted epoch
	epochDecided     bool
	ackEpochs        map[int64]bool
	historyCommitted bool
	newLeaderAcks    map[int64]bool
	established      bool
	abandon          bool
}

type learnerHandler struct {
	id       int64
	conn     transport.Conn
	lastSeen time.Time
}

type proposal struct {
	txn  *Txn
	acks map[int64]bool
}

type syncWaiter struct {
	h         *learnerHandler // nil for a session connected to the leader
	sessionID int64
	xid       int64
}

type trackedSession struct {
	timeout time.Duration
	expires time.Time
	closing bool
}

func newLeaderState(s *Server) *leaderState {
	return &leaderState{
		s:             s,
		learners:      map[int64]*learnerHandler{},
		forwarding:    map[int64]*learnerHandler{},
		records:       map[string]*changeRecord{},
		tracker:       map[int64]*trackedSession{},
		syncWaiters:   map[int64][]syncWaiter{},
		infos:         map[int64]int64{},
		ackEpochs:     map[int64]bool{},
		newLeaderAcks: map[int64]bool{},
	}
}

func (s *Server) saveEpochs() {
	if err := s.cfg.Storage.SaveEpochs(s.acceptedEpoch, s.currentEpoch); err != nil {
		s.lg.Error("saving epochs failed", zlog.Err(err))
	}
}

// lead runs the leader role until the server loses its quorum or stops.
func (s *Server) lead() {
	s.mu.Lock()
	ls := newLeaderState(s)
	s.leader = ls
	ok := s.establish(ls)
	s.mu.Unlock()
	if ok {
		s.leaderLoop(ls)
	}
	s.mu.Lock()
	s.serving.Store(false)
	s.leader = nil
	for _, h := range ls.learners {
		h.conn.Close()
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

// establish runs Zab discovery and synchronization from the leader side.
// Called and returns with s.mu held.
func (s *Server) establish(ls *leaderState) bool {
	ls.infos[s.id] = s.acceptedEpoch
	ls.ackEpochs[s.id] = true
	ls.newLeaderAcks[s.id] = true
	s.lg.Info("★ LEADING: phase 1 (discovery) waiting for a quorum of followers", "quorum", s.quorum)

	// Discovery: a quorum of followers tells us their accepted epochs; the
	// new epoch is larger than any of them.
	if !s.waitCond(s.initTimeout(), func() bool { return len(ls.infos) >= s.quorum }) {
		s.lg.Warn("not enough followers connected in time, giving up leadership", "connected", len(ls.infos))
		return false
	}
	var maxEpoch int64
	for _, e := range ls.infos {
		maxEpoch = max(maxEpoch, e)
	}
	ls.epoch = maxEpoch + 1
	s.acceptedEpoch = ls.epoch
	s.saveEpochs()
	ls.epochDecided = true
	s.cond.Broadcast()
	s.lg.Info("new epoch proposed to followers", "epoch", ls.epoch, "followers", len(ls.infos)-1)

	if !s.waitCond(s.initTimeout(), func() bool { return ls.abandon || len(ls.ackEpochs) >= s.quorum }) || ls.abandon {
		s.lg.Warn("discovery failed (a follower is ahead or not enough acks)", "abandon", ls.abandon)
		return false
	}

	// Our history is the most recent of the quorum: commit all of it before
	// synchronizing followers.
	s.lg.Info("phase 2 (synchronization): committing own history", "upTo", zlog.ZxidString(s.lastLogged),
		"from", zlog.ZxidString(s.lastCommitted))
	s.applyCommitted(s.lastLogged)
	ls.historyCommitted = true
	s.cond.Broadcast()

	if !s.waitCond(s.initTimeout(), func() bool { return len(ls.newLeaderAcks) >= s.quorum }) {
		s.lg.Warn("synchronization failed: no quorum acknowledged NEWLEADER")
		return false
	}
	s.currentEpoch = ls.epoch
	s.saveEpochs()
	ls.established = true
	ls.rebuildTracker()
	for id := range ls.newLeaderAcks {
		if h := ls.learners[id]; h != nil {
			h.conn.Send(&UpToDate{Zxid: s.lastCommitted})
		}
	}
	s.serving.Store(true)
	s.cond.Broadcast()
	var followers []int64
	for id := range ls.forwarding {
		followers = append(followers, id)
	}
	s.lg.Info("★ phase 3 (broadcast): ensemble established, serving clients", "epoch", ls.epoch,
		"followers", fmt.Sprint(followers), "sessions", len(ls.tracker))
	return true
}

func (s *Server) leaderLoop(ls *leaderState) {
	ticker := time.NewTicker(s.cfg.TickTime)
	defer ticker.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
		}
		s.mu.Lock()
		ok := !s.stopping && ls.tick()
		s.mu.Unlock()
		if !ok {
			s.lg.Warn("★ lost contact with a quorum of followers, stepping down")
			return
		}
	}
}

// tick sends heartbeats, checks that a quorum is still alive and expires
// sessions. A leader that cannot hear from a quorum within syncLimit steps
// down, before followers (2*syncLimit) abandon it.
func (ls *leaderState) tick() bool {
	s := ls.s
	now := time.Now()
	alive := 1
	for _, h := range ls.forwarding {
		if now.Sub(h.lastSeen) <= s.syncTimeout() {
			alive++
		} else {
			s.lg.Debug("follower is silent", "follower", h.id, "silentFor", now.Sub(h.lastSeen))
		}
		s.trace("heartbeat to follower", "follower", h.id)
		h.conn.Send(&LeaderPing{Zxid: s.lastCommitted})
	}
	if alive < s.quorum {
		return false
	}
	for _, id := range s.drainTouched() {
		ls.touch(id)
	}
	for id, ts := range ls.tracker {
		if !ts.closing && now.After(ts.expires) {
			ts.closing = true
			s.lg.Info("★ session expired: no server heard from it within its timeout", zlog.Session(id), "timeout", ts.timeout)
			s.propose(ls.closeSessionTxn(&Txn{Zxid: ls.nextZxid(), Time: now.UnixMilli(), SessionID: id}))
		}
	}
	return true
}

func (ls *leaderState) touch(id int64) {
	if ts := ls.tracker[id]; ts != nil && !ts.closing {
		ts.expires = time.Now().Add(ts.timeout)
	}
}

func (ls *leaderState) rebuildTracker() {
	ls.s.dbMu.RLock()
	sessions := ls.s.tree.Sessions()
	ls.s.dbMu.RUnlock()
	now := time.Now()
	for id, t := range sessions {
		d := time.Duration(t) * time.Millisecond
		ls.tracker[id] = &trackedSession{timeout: d, expires: now.Add(d)}
	}
}

func (ls *leaderState) nextZxid() int64 {
	ls.counter++
	return makeZxid(ls.epoch, ls.counter)
}

// leaderProcess turns a client write into a transaction and broadcasts it.
func (s *Server) leaderProcess(f *Forward) {
	ls := s.leader
	txn := ls.prep(f, ls.nextZxid())
	s.lg.Debug("request processor: request -> txn", "op", f.Req.Op.String(), "fromServer", f.ServerID, "txn", txn.Type.String())
	if txn.Type == TxnError {
		s.lg.Info("✗ write rejected by the leader", "op", f.Req.Op.String(), "path", f.Req.Path,
			"reason", txn.Err.Err(), zlog.Session(f.SessionID))
	}
	s.propose(txn)
}

// propose logs a transaction and sends it to the followers.
func (s *Server) propose(txn *Txn) {
	ls := s.leader
	if err := s.appendLog(txn); err != nil {
		s.lg.Error("log append failed", zlog.Err(err))
		return
	}
	s.lg.Debug("PROPOSAL logged and sent to followers", append(txnAttrs(txn), "followers", len(ls.forwarding))...)
	ls.pending = append(ls.pending, &proposal{txn: txn, acks: map[int64]bool{s.id: true}})
	for _, h := range ls.forwarding {
		h.conn.Send(&Proposal{Txn: txn})
	}
	ls.tryCommit()
}

func (ls *leaderState) processAck(from, zxid int64) {
	for _, p := range ls.pending {
		if p.txn.Zxid == zxid {
			p.acks[from] = true
			ls.s.trace("ACK received", "from", from, zlog.Zxid(zxid), "acks", len(p.acks))
			break
		}
	}
	ls.tryCommit()
}

// tryCommit commits, in order, every proposal acknowledged by a quorum.
func (ls *leaderState) tryCommit() {
	s := ls.s
	for len(ls.pending) > 0 && len(ls.pending[0].acks) >= s.quorum {
		p := ls.pending[0]
		ls.pending[0] = nil
		ls.pending = ls.pending[1:]
		z := p.txn.Zxid
		s.lg.Debug("COMMIT: quorum acknowledged", zlog.Zxid(z), "type", p.txn.Type.String(), "acks", len(p.acks), "quorum", s.quorum)
		for _, h := range ls.forwarding {
			h.conn.Send(&Commit{Zxid: z})
		}
		s.applyCommitted(z)
		for _, w := range ls.syncWaiters[z] {
			ls.replySync(w)
		}
		delete(ls.syncWaiters, z)
	}
}

// leaderSync places a sync at the end of the queue between the leader and
// the requesting server: it is answered right after the commit of the last
// outstanding proposal, without broadcasting anything.
func (s *Server) leaderSync(h *learnerHandler, sessionID, xid int64) {
	ls := s.leader
	w := syncWaiter{h: h, sessionID: sessionID, xid: xid}
	s.lg.Debug("sync queued behind outstanding proposals", zlog.Session(sessionID), "outstanding", len(ls.pending))
	if len(ls.pending) == 0 {
		ls.replySync(w)
		return
	}
	last := ls.pending[len(ls.pending)-1].txn.Zxid
	ls.syncWaiters[last] = append(ls.syncWaiters[last], w)
}

func (ls *leaderState) replySync(w syncWaiter) {
	if w.h != nil {
		w.h.conn.Send(&SyncReply{SessionID: w.sessionID, Xid: w.xid})
		return
	}
	ls.s.completeLocal(w.sessionID, w.xid, &zk.Response{Xid: w.xid, Zxid: ls.s.LastZxid()})
}

// applied is called when the leader applies a transaction: outstanding
// change records it superseded are no longer needed.
func (ls *leaderState) applied(txn *Txn) {
	if txn.Type == TxnCloseSession {
		delete(ls.tracker, txn.SessionID)
	}
	for _, p := range affectedPaths(txn) {
		if r := ls.records[p]; r != nil && r.zxid <= txn.Zxid {
			delete(ls.records, p)
		}
	}
}

// serveLearner is the leader side of a follower connection.
func (s *Server) serveLearner(c transport.Conn, info *FollowerInfo) {
	s.mu.Lock()
	ls := s.leader
	if ls == nil || s.state != Leading {
		s.mu.Unlock()
		return
	}
	s.lg.Info("follower connected", "follower", info.ID, "acceptedEpoch", info.AcceptedEpoch,
		"lastLogged", zlog.ZxidString(info.LastLogged))
	h := &learnerHandler{id: info.ID, conn: c, lastSeen: time.Now()}
	if old := ls.learners[info.ID]; old != nil {
		old.conn.Close()
		delete(ls.forwarding, info.ID)
	}
	ls.learners[info.ID] = h
	if !ls.epochDecided {
		ls.infos[info.ID] = info.AcceptedEpoch
		s.cond.Broadcast()
	}
	if !s.waitCond(s.initTimeout(), func() bool { return ls.epochDecided || s.leader != ls }) || s.leader != ls {
		s.mu.Unlock()
		return
	}
	epoch := ls.epoch
	s.mu.Unlock()

	done := make(chan struct{})
	defer close(done)
	in := reader(c, done)
	defer func() {
		s.mu.Lock()
		if ls.learners[h.id] == h {
			delete(ls.learners, h.id)
			delete(ls.forwarding, h.id)
			if s.leader == ls {
				s.lg.Warn("follower disconnected", "follower", h.id, "remaining", len(ls.forwarding))
			}
		}
		s.mu.Unlock()
	}()

	c.Send(&LeaderInfo{Epoch: epoch})
	m, ok := recvTimeout(in, s.initTimeout(), s.quit)
	ae, isAck := m.(*AckEpoch)
	if !ok || !isAck {
		return
	}

	s.mu.Lock()
	if s.leader != ls {
		s.mu.Unlock()
		return
	}
	if !ls.established && (ae.CurrentEpoch > s.currentEpoch ||
		(ae.CurrentEpoch == s.currentEpoch && ae.LastLogged > s.lastLogged)) {
		// A follower has a more recent history: we must not lead.
		s.lg.Warn("follower has a more recent history than us, abandoning leadership", "follower", h.id)
		ls.abandon = true
		s.cond.Broadcast()
		s.mu.Unlock()
		return
	}
	ls.ackEpochs[h.id] = true
	s.cond.Broadcast()
	if !s.waitCond(s.initTimeout(), func() bool { return ls.historyCommitted || ls.abandon || s.leader != ls }) ||
		ls.abandon || s.leader != ls {
		s.mu.Unlock()
		return
	}

	// Synchronization: send the follower what it misses (DIFF) or, if its
	// history diverged or is too old, a full snapshot (SNAP). In-flight
	// proposals are included; from now on the follower receives every
	// proposal and commit, in order, on this connection.
	if ae.LastLogged == s.logBase || s.logContains(ae.LastLogged) {
		entries := append([]*Txn(nil), s.log[s.logIndexAfter(ae.LastLogged):]...)
		s.lg.Info("synchronizing follower with DIFF", "follower", h.id, "followerLast", zlog.ZxidString(ae.LastLogged),
			"missingTxns", len(entries))
		c.Send(&SyncDiff{Entries: entries})
	} else {
		s.dbMu.RLock()
		snap := s.tree.ConsistentSnapshot()
		s.dbMu.RUnlock()
		entries := append([]*Txn(nil), s.log[s.logIndexAfter(s.lastCommitted):]...)
		s.lg.Info("synchronizing follower with SNAP (history diverged or too old)", "follower", h.id,
			"followerLast", zlog.ZxidString(ae.LastLogged), "snapshotZxid", zlog.ZxidString(snap.Zxid), "znodes", len(snap.Nodes))
		c.Send(&SyncSnap{Snap: snap, Entries: entries})
	}
	c.Send(&NewLeader{Epoch: ls.epoch, CommitZxid: s.lastCommitted})
	ls.forwarding[h.id] = h
	h.lastSeen = time.Now()
	s.mu.Unlock()

	for m := range in {
		s.mu.Lock()
		if s.leader != ls {
			s.mu.Unlock()
			return
		}
		h.lastSeen = time.Now()
		switch m := m.(type) {
		case *AckNewLeader:
			s.lg.Info("follower acknowledged NEWLEADER", "follower", h.id, "established", ls.established)
			ls.newLeaderAcks[h.id] = true
			if ls.established {
				c.Send(&UpToDate{Zxid: s.lastCommitted})
			}
			s.cond.Broadcast()
		case *Ack:
			ls.processAck(h.id, m.Zxid)
		case *Forward:
			s.lg.Debug("write forwarded by follower", "follower", h.id, "op", m.Req.Op.String(), "path", m.Req.Path, zlog.Session(m.SessionID))
			if ls.established {
				s.leaderProcess(m)
			}
		case *SyncRequest:
			if ls.established {
				s.leaderSync(h, m.SessionID, m.Xid)
			}
		case *PingReply:
			s.trace("heartbeat reply", "follower", h.id, "activeSessions", len(m.Sessions))
			for _, id := range m.Sessions {
				ls.touch(id)
			}
		}
		s.mu.Unlock()
	}
}

// reader pumps messages from a connection into a channel until the
// connection fails or done is closed.
func reader(c transport.Conn, done <-chan struct{}) <-chan any {
	ch := make(chan any, 256)
	go func() {
		defer close(ch)
		for {
			m, err := c.Recv()
			if err != nil {
				return
			}
			select {
			case ch <- m:
			case <-done:
				return
			}
		}
	}()
	return ch
}
