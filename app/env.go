// Package app contains real-world applications built on the ZooKeeper
// client and recipes. Each scenario is a reusable component that runs either
// inside the in-process demo (zkapp demo) or as a standalone process
// connected to a real ensemble over TCP (one terminal per process).
//
//   - config.go     dynamic configuration pushed to running services
//   - discovery.go  service registry + client-side load balancing gateway
//   - scheduler.go  leader election: exactly one active job scheduler
//   - tickets.go    distributed lock guarding a non-transactional resource
//   - queue.go      fault-tolerant work queue with ephemeral task claims
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"zookeeper/client"
	"zookeeper/server"
	"zookeeper/transport"
	"zookeeper/zk"
	"zookeeper/zlog"
)

// Env describes how application processes reach the ensemble.
type Env struct {
	Servers        []string
	Transport      transport.Transport
	Sink           *zlog.Sink
	Level          slog.Leveler // level of application and client logs (may change at runtime)
	SessionTimeout time.Duration
}

// Process is one application process (a service instance, a worker...)
// with its own ZooKeeper session. In the in-process demo a process can be
// crashed: it vanishes without saying goodbye to ZooKeeper, exactly like a
// killed machine, so its session only ends when the timeout expires.
type Process struct {
	Name   string
	Client *client.Client
	Log    *slog.Logger
	Ctx    context.Context

	// Kind and Note describe the process in monitoring UIs.
	Kind    string
	Note    string
	Started time.Time

	cancel context.CancelFunc
	tr     *crashableTransport
	mute   func()
	once   sync.Once
	status atomic.Value // "running", "stopped", "crashed"
}

// Status returns "running", "stopped" or "crashed".
func (p *Process) Status() string {
	if v, ok := p.status.Load().(string); ok {
		return v
	}
	return "running"
}

// Start connects a new process to the ensemble.
func (e *Env) Start(parent context.Context, name string) (*Process, error) {
	base := e.Sink.Logger(e.Level)
	lg, mute := zlog.NewMutable(base)
	tr := &crashableTransport{inner: e.Transport}
	timeout := e.SessionTimeout
	if timeout == 0 {
		timeout = 4 * time.Second
	}
	c, err := client.Connect(client.Config{
		Servers:        e.Servers,
		Transport:      tr,
		SessionTimeout: timeout,
		ConnectTimeout: 3 * timeout,
		Name:           name,
		Logger:         lg,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: connect: %w", name, err)
	}
	ctx, cancel := context.WithCancel(parent)
	return &Process{Name: name, Client: c, Log: c.Logger(), Ctx: ctx, cancel: cancel, tr: tr, mute: mute,
		Started: time.Now()}, nil
}

// Stop shuts the process down gracefully: the session is closed, so its
// ephemeral znodes disappear immediately.
func (p *Process) Stop() {
	p.once.Do(func() {
		p.Log.Info("★ process stopping gracefully (closing its ZooKeeper session)")
		p.status.Store("stopped")
		p.cancel()
		p.Client.Close()
	})
}

// Crash kills the process abruptly: its connections drop and it never
// closes its session. ZooKeeper notices only when the session times out,
// then removes its ephemeral znodes.
func (p *Process) Crash() {
	p.once.Do(func() {
		p.Log.Warn("💥 process CRASHED (no goodbye to ZooKeeper: its session will expire after the timeout)",
			"sessionTimeout", p.Client.SessionTimeout())
		p.status.Store("crashed")
		p.mute()
		p.cancel()
		p.tr.crash()
		go p.Client.Close() // reclaim goroutines; nothing reaches the servers
	})
}

// crashableTransport can cut every connection of a process at once.
type crashableTransport struct {
	inner   transport.Transport
	mu      sync.Mutex
	crashed bool
	conns   []transport.Conn
}

func (t *crashableTransport) Listen(addr string) (transport.Listener, error) {
	return t.inner.Listen(addr)
}

func (t *crashableTransport) Dial(addr string, d time.Duration) (transport.Conn, error) {
	t.mu.Lock()
	crashed := t.crashed
	t.mu.Unlock()
	if crashed {
		return nil, errors.New("process crashed")
	}
	c, err := t.inner.Dial(addr, d)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.crashed {
		c.Close()
		return nil, errors.New("process crashed")
	}
	t.conns = append(t.conns, c)
	return c, nil
}

func (t *crashableTransport) crash() {
	t.mu.Lock()
	t.crashed = true
	conns := t.conns
	t.conns = nil
	t.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// ---------------------------------------------------------------------------
// Local ensemble

// Ensemble is a set of ZooKeeper servers running in this process.
type Ensemble struct {
	Peers    map[int64]string
	Servers  map[int64]*server.Server
	storages map[int64]server.Storage
	tr       transport.Transport
	lg       *slog.Logger
	tick     time.Duration
	mu       sync.Mutex
}

// EnsembleConfig configures StartEnsemble.
type EnsembleConfig struct {
	Peers     map[int64]string
	Transport transport.Transport
	Logger    *slog.Logger
	Tick      time.Duration
	// DataDir, if set, makes the servers durable (one sub-directory each).
	DataDir string
}

// StartEnsemble starts every server and waits until a leader serves.
func StartEnsemble(cfg EnsembleConfig) (*Ensemble, error) {
	e := &Ensemble{Peers: cfg.Peers, Servers: map[int64]*server.Server{}, storages: map[int64]server.Storage{},
		tr: cfg.Transport, lg: cfg.Logger, tick: cfg.Tick}
	for id := range cfg.Peers {
		if cfg.DataDir != "" {
			st, err := server.NewFileStorage(fmt.Sprintf("%s/server-%d", cfg.DataDir, id), true)
			if err != nil {
				return nil, err
			}
			e.storages[id] = st
		} else {
			e.storages[id] = server.NewMemStorage()
		}
	}
	for _, id := range e.IDs() {
		if err := e.StartServer(id); err != nil {
			e.Close()
			return nil, err
		}
	}
	if !e.WaitReady(30 * time.Second) {
		e.Close()
		return nil, errors.New("ensemble did not elect a leader in time")
	}
	return e, nil
}

// NewEnsemble creates an empty ensemble: no server is configured or running
// yet. Add servers with AddServer.
func NewEnsemble(cfg EnsembleConfig) *Ensemble {
	return &Ensemble{Peers: map[int64]string{}, Servers: map[int64]*server.Server{},
		storages: map[int64]server.Storage{}, tr: cfg.Transport, lg: cfg.Logger, tick: cfg.Tick}
}

// IDs returns all configured server ids in order.
func (e *Ensemble) IDs() []int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var ids []int64
	for id := range e.Peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Addr returns the address of a configured server ("" if unknown).
func (e *Ensemble) Addr(id int64) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Peers[id]
}

// Size returns the number of configured servers.
func (e *Ensemble) Size() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.Peers)
}

// StartServer (re)starts one server on its existing storage.
func (e *Ensemble) StartServer(id int64) error {
	e.mu.Lock()
	peers := make(map[int64]string, len(e.Peers))
	for k, v := range e.Peers {
		peers[k] = v
	}
	st := e.storages[id]
	e.mu.Unlock()
	if st == nil {
		return fmt.Errorf("server %d is not part of the ensemble", id)
	}
	s, err := server.New(server.Config{ID: id, Peers: peers, Transport: e.tr, Storage: st,
		TickTime: e.tick, Logger: e.lg})
	if err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}
	e.mu.Lock()
	e.Servers[id] = s
	e.mu.Unlock()
	return nil
}

// AddServer adds a new member with empty storage and starts it. Membership
// is static in ZooKeeper (as in releases before dynamic reconfiguration):
// every running server is restarted with the new configuration (a rolling
// restart). Their data survives in their storage. It returns the new id.
func (e *Ensemble) AddServer() (int64, error) {
	ids, err := e.Grow(1)
	if len(ids) == 0 {
		return 0, err
	}
	return ids[0], err
}

// Grow adds n members at once (one rolling restart) and starts them.
func (e *Ensemble) Grow(n int) ([]int64, error) {
	e.mu.Lock()
	var next int64 = 1
	for k := range e.Peers {
		next = max(next, k+1)
	}
	var ids []int64
	for i := 0; i < n; i++ {
		id := next + int64(i)
		e.Peers[id] = fmt.Sprintf("zk-%d", id)
		e.storages[id] = server.NewMemStorage()
		ids = append(ids, id)
	}
	e.mu.Unlock()
	if err := e.restartRunning(); err != nil {
		return ids, err
	}
	for _, id := range ids {
		if err := e.StartServer(id); err != nil {
			return ids, err
		}
	}
	return ids, nil
}

// RemoveServer stops a member, discards its storage and restarts the other
// running servers with the smaller configuration.
func (e *Ensemble) RemoveServer(id int64) error {
	e.StopServer(id)
	e.mu.Lock()
	if _, ok := e.Peers[id]; !ok {
		e.mu.Unlock()
		return fmt.Errorf("server %d is not part of the ensemble", id)
	}
	delete(e.Peers, id)
	if st := e.storages[id]; st != nil {
		st.Close()
	}
	delete(e.storages, id)
	e.mu.Unlock()
	return e.restartRunning()
}

func (e *Ensemble) restartRunning() error {
	e.mu.Lock()
	var running []int64
	for id := range e.Servers {
		running = append(running, id)
	}
	e.mu.Unlock()
	sort.Slice(running, func(i, j int) bool { return running[i] < running[j] })
	for _, id := range running {
		e.StopServer(id)
	}
	for _, id := range running {
		if err := e.StartServer(id); err != nil {
			return err
		}
	}
	return nil
}

// StopServer crashes one server (its storage survives).
func (e *Ensemble) StopServer(id int64) {
	e.mu.Lock()
	s := e.Servers[id]
	delete(e.Servers, id)
	e.mu.Unlock()
	if s != nil {
		s.Close()
	}
}

// Server returns a running server, or nil if it is stopped.
func (e *Ensemble) Server(id int64) *server.Server {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Servers[id]
}

// Leader returns the id of the serving leader, 0 if none.
func (e *Ensemble) Leader() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id, s := range e.Servers {
		if s.State() == server.Leading && s.Serving() {
			return id
		}
	}
	return 0
}

// WaitReady waits until every running server serves with the same leader.
func (e *Ensemble) WaitReady(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		ok, leader := true, int64(0)
		for _, s := range e.Servers {
			l := s.LeaderID()
			if !s.Serving() || l == 0 || (leader != 0 && l != leader) {
				ok = false
				break
			}
			leader = l
		}
		e.mu.Unlock()
		if ok && leader != 0 {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// Addrs returns the client addresses of all servers.
func (e *Ensemble) Addrs() []string {
	var out []string
	for _, id := range e.IDs() {
		out = append(out, e.Addr(id))
	}
	return out
}

// Close stops every server.
func (e *Ensemble) Close() {
	for _, id := range e.IDs() {
		e.StopServer(id)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, st := range e.storages {
		st.Close()
	}
}

// ---------------------------------------------------------------------------
// Tree printing

// PrintTree prints the znode tree under root, marking ephemeral znodes.
func PrintTree(c *client.Client, root string, print func(string)) error {
	if err := c.Sync(root); err != nil {
		return err
	}
	return printNode(c, root, 0, print)
}

func printNode(c *client.Client, p string, depth int, print func(string)) error {
	data, st, err := c.GetData(p, false)
	if errors.Is(err, zk.ErrNoNode) {
		return nil
	}
	if err != nil {
		return err
	}
	name := zk.Base(p)
	if depth == 0 {
		name = p
	}
	line := strings.Repeat("   ", depth) + "└─ " + name
	var tags []string
	if st.EphemeralOwner != 0 {
		tags = append(tags, "ephemeral, owner "+zlog.SessionString(st.EphemeralOwner))
	}
	if len(data) > 0 {
		d := string(data)
		if len(d) > 60 {
			d = d[:57] + "..."
		}
		tags = append(tags, fmt.Sprintf("data=%q", d))
	}
	tags = append(tags, fmt.Sprintf("v%d", st.Version))
	print(line + "  (" + strings.Join(tags, ", ") + ")")
	kids, err := c.GetChildren(p, false)
	if err != nil {
		return nil
	}
	for _, k := range kids {
		if err := printNode(c, zk.Join(p, k), depth+1, print); err != nil {
			return err
		}
	}
	return nil
}

// sleepCtx sleeps unless the context ends first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
