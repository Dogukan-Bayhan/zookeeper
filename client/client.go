// Package client is the ZooKeeper client library (Section 2.2).
//
// A Client owns a session with the ensemble. It connects to one server at a
// time, sends heartbeats when idle, and transparently moves the session to
// another server when the connection breaks. Every operation is available
// in a synchronous and an asynchronous form; callbacks and watch
// notifications are delivered by a single goroutine in the order the
// corresponding responses and events arrived, so the FIFO client order
// guarantee extends to the application.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"zookeeper/transport"
	"zookeeper/zk"
	"zookeeper/zlog"
)

// Config configures a client.
type Config struct {
	// Servers lists the addresses of the ensemble members.
	Servers []string
	// SessionTimeout is the requested session timeout (default 10s).
	SessionTimeout time.Duration
	// Transport defaults to TCP.
	Transport transport.Transport
	// Watcher receives session events and the notifications of watches set
	// with watch=true. It runs on the callback goroutine and must not block
	// or call synchronous client methods.
	Watcher func(zk.Event)
	// ConnectTimeout bounds how long Connect waits for the first session
	// (default: SessionTimeout).
	ConnectTimeout time.Duration
	// Name identifies the client in logs (default "client").
	Name string
	// Logger receives logs of every step: connection attempts, session
	// establishment, requests, responses, watches; nil disables logging.
	Logger *slog.Logger
}

type watchKind int

const (
	noWatch watchKind = iota
	watchData
	watchExist
	watchChild
)

type watcher struct {
	ch  chan zk.Event // nil for the default watcher
	def bool
}

type call struct {
	req  *zk.Request
	kind watchKind
	w    *watcher
	cb   func(*zk.Response, error)
}

// Client is a ZooKeeper session.
type Client struct {
	cfg    Config
	lg     *slog.Logger
	events *dispatcher
	kick   chan struct{} // closed by Close
	done   chan struct{} // closed when the connection loop exits
	first  chan struct{} // closed on first session establishment or failure

	mu        sync.Mutex
	state     zk.State
	closing   bool
	sessionID int64
	timeout   time.Duration
	lastZxid  int64
	conn      transport.Conn
	xid       int64
	sent      []*call // awaiting a response, in order
	unsent    []*call // issued while disconnected
	server    string  // address of the current server
	lastRecv  time.Time
	lastSend  time.Time
	firstOnce sync.Once
	dataW     map[string][]*watcher
	existW    map[string][]*watcher
	childW    map[string][]*watcher
}

// Connect creates a session with the ensemble and waits until it is
// established.
func Connect(cfg Config) (*Client, error) {
	if len(cfg.Servers) == 0 {
		return nil, errors.New("client: no servers")
	}
	if cfg.SessionTimeout <= 0 {
		cfg.SessionTimeout = 10 * time.Second
	}
	if cfg.Transport == nil {
		cfg.Transport = transport.TCP{}
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = cfg.SessionTimeout
	}
	if cfg.Name == "" {
		cfg.Name = "client"
	}
	c := &Client{
		cfg:     cfg,
		lg:      zlog.OrDiscard(cfg.Logger).With("comp", cfg.Name),
		events:  newDispatcher(),
		kick:    make(chan struct{}),
		done:    make(chan struct{}),
		first:   make(chan struct{}),
		state:   zk.StateConnecting,
		timeout: cfg.SessionTimeout,
		dataW:   map[string][]*watcher{},
		existW:  map[string][]*watcher{},
		childW:  map[string][]*watcher{},
	}
	c.lg.Info("connecting to ensemble", "servers", fmt.Sprint(cfg.Servers), "sessionTimeout", cfg.SessionTimeout)
	go c.loop()
	select {
	case <-c.first:
	case <-time.After(cfg.ConnectTimeout):
	}
	if c.State() != zk.StateConnected {
		c.lg.Error("could not establish a session", "after", cfg.ConnectTimeout)
		c.Close()
		return nil, zk.ErrConnectionLoss
	}
	return c, nil
}

// Logger returns the client's logger; recipes built on the client log
// through it.
func (c *Client) Logger() *slog.Logger { return c.lg }

// Name returns the client name used in logs.
func (c *Client) Name() string { return c.cfg.Name }

// Server returns the address of the server the client is connected to.
func (c *Client) Server() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.server
}

// SessionID returns the session id.
func (c *Client) SessionID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// State returns the session state.
func (c *Client) State() zk.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// LastZxid returns the most recent zxid observed by the client.
func (c *Client) LastZxid() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastZxid
}

// SessionTimeout returns the timeout negotiated with the server.
func (c *Client) SessionTimeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timeout
}

// Close ends the session: ephemeral znodes are removed and watches dropped.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		<-c.done
		return nil
	}
	c.closing = true
	live := c.state == zk.StateConnected
	sid := c.sessionID
	c.mu.Unlock()
	c.lg.Info("closing session (ephemeral znodes will be removed)", zlog.Session(sid))

	if live {
		ch := make(chan struct{}, 1)
		c.do(&zk.Request{Op: zk.OpCloseSession}, noWatch, nil, func(*zk.Response, error) { ch <- struct{}{} })
		select {
		case <-ch:
		case <-time.After(c.cfg.SessionTimeout):
		}
	}

	c.mu.Lock()
	if c.state != zk.StateExpired {
		c.state = zk.StateClosed
	}
	pending := append(c.sent, c.unsent...)
	c.sent, c.unsent = nil, nil
	conn := c.conn
	c.mu.Unlock()
	for _, cl := range pending {
		c.fail(cl, zk.ErrClosed)
	}
	if conn != nil {
		conn.Close()
	}
	close(c.kick)
	<-c.done
	c.firstOnce.Do(func() { close(c.first) })
	c.events.stop()
	return nil
}

// ---- connection management ----

func (c *Client) sleep(d time.Duration) bool {
	select {
	case <-c.kick:
		return false
	case <-time.After(d):
		return true
	}
}

func (c *Client) loop() {
	defer close(c.done)
	servers := slices.Clone(c.cfg.Servers)
	rand.Shuffle(len(servers), func(i, j int) { servers[i], servers[j] = servers[j], servers[i] })
	backoff := min(c.cfg.SessionTimeout/20, 100*time.Millisecond)
	for i := 0; ; i++ {
		c.mu.Lock()
		stop := c.closing || c.state == zk.StateExpired
		c.mu.Unlock()
		if stop {
			return
		}
		if i > 0 && i%len(servers) == 0 && !c.sleep(backoff) {
			return
		}
		addr := servers[i%len(servers)]
		c.lg.Debug("dialing server", "server", addr)
		conn, err := c.cfg.Transport.Dial(addr, c.cfg.SessionTimeout/3)
		if err != nil {
			c.lg.Debug("server unreachable, trying next", "server", addr, zlog.Err(err))
			continue
		}
		switch c.handshake(conn, addr) {
		case handshakeExpired:
			conn.Close()
			c.expire()
			return
		case handshakeRetry:
			conn.Close()
			continue
		}
		c.serve(conn)
		conn.Close()
		c.disconnected()
	}
}

type handshakeResult int

const (
	handshakeOK handshakeResult = iota
	handshakeRetry
	handshakeExpired
)

func (c *Client) watchPaths(m map[string][]*watcher) []string {
	out := make([]string, 0, len(m))
	for p := range m {
		out = append(out, p)
	}
	return out
}

func (c *Client) handshake(conn transport.Conn, addr string) handshakeResult {
	c.mu.Lock()
	req := &zk.ConnectRequest{
		SessionID:    c.sessionID,
		Timeout:      c.cfg.SessionTimeout.Milliseconds(),
		LastZxid:     c.lastZxid,
		DataWatches:  c.watchPaths(c.dataW),
		ExistWatches: c.watchPaths(c.existW),
		ChildWatches: c.watchPaths(c.childW),
	}
	c.mu.Unlock()
	if conn.Send(req) != nil {
		return handshakeRetry
	}
	type result struct {
		m   any
		err error
	}
	ch := make(chan result, 1)
	go func() {
		m, err := conn.Recv()
		ch <- result{m, err}
	}()
	var r result
	select {
	case r = <-ch:
	case <-time.After(c.cfg.SessionTimeout / 3):
		return handshakeRetry
	case <-c.kick:
		return handshakeRetry
	}
	resp, ok := r.m.(*zk.ConnectResponse)
	if r.err != nil || !ok {
		c.lg.Debug("handshake failed", "server", addr)
		return handshakeRetry
	}
	switch resp.Err {
	case zk.CodeOK:
	case zk.CodeSessionExpired:
		c.lg.Error("★ server says our session has EXPIRED", "server", addr, zlog.Session(req.SessionID))
		return handshakeExpired
	default:
		c.lg.Info("server refused the session, trying another", "server", addr, "reason", resp.Err.Err())
		return handshakeRetry
	}

	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return handshakeRetry
	}
	c.sessionID = resp.SessionID
	c.timeout = time.Duration(resp.Timeout) * time.Millisecond
	c.lastZxid = max(c.lastZxid, resp.Zxid)
	c.conn = conn
	c.server = addr
	c.state = zk.StateConnected
	if req.SessionID == 0 {
		c.lg.Info("★ session established", "server", addr, zlog.Session(resp.SessionID),
			"timeout", c.timeout)
	} else {
		c.lg.Info("★ session re-established on another server", "server", addr, zlog.Session(resp.SessionID),
			"lastZxid", zlog.ZxidString(req.LastZxid), "watchesReRegistered",
			len(req.DataWatches)+len(req.ExistWatches)+len(req.ChildWatches), "queuedRequests", len(c.unsent))
	}
	c.lastRecv, c.lastSend = time.Now(), time.Now()
	for _, cl := range c.unsent {
		c.sent = append(c.sent, cl)
		conn.Send(cl.req)
	}
	c.unsent = nil
	c.mu.Unlock()
	c.firstOnce.Do(func() { close(c.first) })
	c.sessionEvent(zk.StateConnected)
	return handshakeOK
}

// serve runs a connection until it breaks or the server goes silent. The
// client sends a heartbeat after s/3 of idleness and switches servers after
// 2s/3 without hearing anything (Section 4.4).
func (c *Client) serve(conn transport.Conn) {
	broken := make(chan struct{})
	go func() {
		defer close(broken)
		for {
			m, err := conn.Recv()
			if err != nil || !c.handleMsg(m) {
				return
			}
		}
	}()
	c.mu.Lock()
	timeout := c.timeout
	c.mu.Unlock()
	ticker := time.NewTicker(max(timeout/10, time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-broken:
			return
		case <-c.kick:
			return
		case <-ticker.C:
		}
		c.mu.Lock()
		now := time.Now()
		if now.Sub(c.lastRecv) > 2*timeout/3 {
			c.mu.Unlock()
			c.lg.Warn("server silent for 2/3 of the session timeout, switching server", "server", c.Server(),
				"silentFor", now.Sub(c.lastRecv))
			return
		}
		if now.Sub(c.lastSend) >= timeout/3 {
			c.lg.Log(context.Background(), zlog.LevelTrace, "idle for 1/3 of the session timeout, sending heartbeat")
			conn.Send(&zk.Request{Xid: zk.PingXid, Op: zk.OpPing})
			c.lastSend = now
		}
		c.mu.Unlock()
	}
}

func (c *Client) handleMsg(m any) bool {
	switch m := m.(type) {
	case *zk.Response:
		c.mu.Lock()
		c.lastRecv = time.Now()
		c.lastZxid = max(c.lastZxid, m.Zxid)
		if m.Xid == zk.PingXid {
			c.mu.Unlock()
			return true
		}
		if len(c.sent) == 0 || c.sent[0].req.Xid != m.Xid {
			c.mu.Unlock()
			c.lg.Error("protocol violation: response out of order", "xid", m.Xid)
			return false // protocol violation: responses must be in order
		}
		cl := c.sent[0]
		c.sent[0] = nil
		c.sent = c.sent[1:]
		c.registerWatch(cl, m)
		c.mu.Unlock()
		c.lg.Debug("← response", "op", cl.req.Op.String(), "path", cl.req.Path, "xid", m.Xid,
			"result", m.Err.Err(), zlog.Zxid(m.Zxid))
		c.events.push(func() { cl.cb(m, m.Err.Err()) })
	case *zk.WatcherEvent:
		c.mu.Lock()
		c.lastRecv = time.Now()
		c.lastZxid = max(c.lastZxid, m.Zxid)
		ws := c.takeWatchers(m.Path, m.Type)
		c.mu.Unlock()
		c.lg.Debug("⚡ watch event received", "event", m.Type.String(), "path", m.Path, "watchers", len(ws))
		ev := zk.Event{Type: m.Type, State: zk.StateConnected, Path: m.Path}
		c.events.push(func() {
			for _, w := range ws {
				c.deliver(w, ev)
			}
		})
	}
	return true
}

func (c *Client) disconnected() {
	c.mu.Lock()
	c.conn = nil
	failed := c.sent
	c.sent = nil
	notify := !c.closing && c.state == zk.StateConnected
	if notify {
		c.state = zk.StateDisconnected
	}
	c.mu.Unlock()
	// The outcome of requests in flight is unknown.
	for _, cl := range failed {
		c.fail(cl, zk.ErrConnectionLoss)
	}
	if notify {
		c.lg.Warn("★ connection lost, will reconnect to another server", "failedInFlight", len(failed))
		c.sessionEvent(zk.StateDisconnected)
	}
}

func (c *Client) expire() {
	c.mu.Lock()
	c.state = zk.StateExpired
	pending := append(c.sent, c.unsent...)
	c.sent, c.unsent = nil, nil
	var ws []*watcher
	for _, m := range []map[string][]*watcher{c.dataW, c.existW, c.childW} {
		for p, list := range m {
			ws = append(ws, list...)
			delete(m, p)
		}
	}
	c.mu.Unlock()
	c.firstOnce.Do(func() { close(c.first) })
	c.lg.Error("★ session expired: ephemeral znodes are gone, watches dropped", "failedRequests", len(pending),
		"watchers", len(ws))
	for _, cl := range pending {
		c.fail(cl, zk.ErrSessionExpired)
	}
	ev := zk.Event{Type: zk.EventSession, State: zk.StateExpired, Err: zk.ErrSessionExpired}
	c.events.push(func() {
		for _, w := range ws {
			if !w.def {
				c.deliver(w, ev)
			}
		}
		if c.cfg.Watcher != nil {
			c.cfg.Watcher(ev)
		}
	})
}

func (c *Client) sessionEvent(st zk.State) {
	if c.cfg.Watcher == nil {
		return
	}
	ev := zk.Event{Type: zk.EventSession, State: st}
	c.events.push(func() { c.cfg.Watcher(ev) })
}

// ---- watches ----

func (c *Client) registerWatch(cl *call, r *zk.Response) {
	if cl.w == nil {
		return
	}
	var m map[string][]*watcher
	switch {
	case cl.kind == watchExist && r.Err == zk.CodeOK:
		m = c.dataW
	case cl.kind == watchExist && r.Err == zk.CodeNoNode:
		m = c.existW
	case cl.kind == watchData && r.Err == zk.CodeOK:
		m = c.dataW
	case cl.kind == watchChild && r.Err == zk.CodeOK:
		m = c.childW
	default:
		return
	}
	m[cl.req.Path] = append(m[cl.req.Path], cl.w)
	c.lg.Debug("watch registered", "path", cl.req.Path, "kind", [...]string{"", "data", "exists", "children"}[cl.kind])
}

func (c *Client) takeWatchers(path string, t zk.EventType) []*watcher {
	var out []*watcher
	take := func(m map[string][]*watcher) {
		out = append(out, m[path]...)
		delete(m, path)
	}
	switch t {
	case zk.EventNodeCreated, zk.EventNodeDataChanged:
		take(c.dataW)
		take(c.existW)
	case zk.EventNodeChildrenChanged:
		take(c.childW)
	case zk.EventNodeDeleted:
		take(c.dataW)
		take(c.existW)
		take(c.childW)
	}
	return out
}

func (c *Client) deliver(w *watcher, ev zk.Event) {
	if w.def {
		if c.cfg.Watcher != nil {
			c.cfg.Watcher(ev)
		}
		return
	}
	select {
	case w.ch <- ev:
	default:
	}
}

// ---- requests ----

func (c *Client) fail(cl *call, err error) {
	c.events.push(func() { cl.cb(nil, err) })
}

func (c *Client) do(req *zk.Request, kind watchKind, w *watcher, cb func(*zk.Response, error)) {
	cl := &call{req: req, kind: kind, w: w, cb: cb}
	c.mu.Lock()
	var err error
	switch {
	case c.state == zk.StateExpired:
		err = zk.ErrSessionExpired
	case c.state == zk.StateClosed, c.closing && req.Op != zk.OpCloseSession:
		err = zk.ErrClosed
	}
	if err != nil {
		c.mu.Unlock()
		c.fail(cl, err)
		return
	}
	c.xid++
	req.Xid = c.xid
	queued := false
	if c.conn != nil && c.state == zk.StateConnected {
		c.sent = append(c.sent, cl)
		c.conn.Send(req)
		c.lastSend = time.Now()
	} else {
		c.unsent = append(c.unsent, cl)
		queued = true
	}
	c.mu.Unlock()
	if queued {
		c.lg.Debug("→ request queued until reconnected", "op", req.Op.String(), "path", req.Path, "xid", req.Xid)
	} else {
		c.lg.Debug("→ request", "op", req.Op.String(), "path", req.Path, "xid", req.Xid, "watch", req.Watch)
	}
}

type result struct {
	r   *zk.Response
	err error
}

func (c *Client) roundTrip(req *zk.Request, kind watchKind, w *watcher) (*zk.Response, error) {
	ch := make(chan result, 1)
	c.do(req, kind, w, func(r *zk.Response, err error) { ch <- result{r, err} })
	res := <-ch
	return res.r, res.err
}

func (c *Client) watcherFor(watch bool) *watcher {
	if watch {
		return &watcher{def: true}
	}
	return nil
}

func newChanWatcher() *watcher { return &watcher{ch: make(chan zk.Event, 1)} }

// Create creates a znode and returns its name, which includes the sequence
// number for sequential znodes.
func (c *Client) Create(path string, data []byte, flags zk.CreateFlag) (string, error) {
	r, err := c.roundTrip(&zk.Request{Op: zk.OpCreate, Path: path, Data: slices.Clone(data), Flags: flags}, noWatch, nil)
	if err != nil {
		return "", err
	}
	return r.Path, nil
}

// CreateAsync is the asynchronous form of Create.
func (c *Client) CreateAsync(path string, data []byte, flags zk.CreateFlag, cb func(string, error)) {
	c.do(&zk.Request{Op: zk.OpCreate, Path: path, Data: slices.Clone(data), Flags: flags}, noWatch, nil,
		func(r *zk.Response, err error) {
			if err != nil {
				cb("", err)
				return
			}
			cb(r.Path, nil)
		})
}

// Delete deletes a znode if it is at the expected version (AnyVersion skips
// the check).
func (c *Client) Delete(path string, version int32) error {
	_, err := c.roundTrip(&zk.Request{Op: zk.OpDelete, Path: path, Version: version}, noWatch, nil)
	return err
}

// DeleteAsync is the asynchronous form of Delete.
func (c *Client) DeleteAsync(path string, version int32, cb func(error)) {
	c.do(&zk.Request{Op: zk.OpDelete, Path: path, Version: version}, noWatch, nil,
		func(_ *zk.Response, err error) { cb(err) })
}

// Exists returns the znode metadata, or nil if it does not exist. With
// watch, the default watcher is notified when the znode is created, deleted
// or its data changes.
func (c *Client) Exists(path string, watch bool) (*zk.Stat, error) {
	return c.exists(path, c.watcherFor(watch))
}

// ExistsW is Exists with a one-time watch delivered on the returned channel.
func (c *Client) ExistsW(path string) (*zk.Stat, <-chan zk.Event, error) {
	w := newChanWatcher()
	st, err := c.exists(path, w)
	if err != nil {
		return nil, nil, err
	}
	return st, w.ch, nil
}

func (c *Client) exists(path string, w *watcher) (*zk.Stat, error) {
	r, err := c.roundTrip(&zk.Request{Op: zk.OpExists, Path: path, Watch: w != nil}, watchExist, w)
	if errors.Is(err, zk.ErrNoNode) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return r.Stat, nil
}

// ExistsAsync is the asynchronous form of Exists.
func (c *Client) ExistsAsync(path string, watch bool, cb func(*zk.Stat, error)) {
	c.do(&zk.Request{Op: zk.OpExists, Path: path, Watch: watch}, watchExist, c.watcherFor(watch),
		func(r *zk.Response, err error) {
			switch {
			case errors.Is(err, zk.ErrNoNode):
				cb(nil, nil)
			case err != nil:
				cb(nil, err)
			default:
				cb(r.Stat, nil)
			}
		})
}

// GetData returns the data and metadata of a znode. The watch is not set if
// the znode does not exist.
func (c *Client) GetData(path string, watch bool) ([]byte, *zk.Stat, error) {
	r, err := c.roundTrip(&zk.Request{Op: zk.OpGetData, Path: path, Watch: watch}, watchData, c.watcherFor(watch))
	if err != nil {
		return nil, nil, err
	}
	return r.Data, r.Stat, nil
}

// GetDataW is GetData with a one-time watch delivered on the returned channel.
func (c *Client) GetDataW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	w := newChanWatcher()
	r, err := c.roundTrip(&zk.Request{Op: zk.OpGetData, Path: path, Watch: true}, watchData, w)
	if err != nil {
		return nil, nil, nil, err
	}
	return r.Data, r.Stat, w.ch, nil
}

// GetDataAsync is the asynchronous form of GetData.
func (c *Client) GetDataAsync(path string, watch bool, cb func([]byte, *zk.Stat, error)) {
	c.do(&zk.Request{Op: zk.OpGetData, Path: path, Watch: watch}, watchData, c.watcherFor(watch),
		func(r *zk.Response, err error) {
			if err != nil {
				cb(nil, nil, err)
				return
			}
			cb(r.Data, r.Stat, nil)
		})
}

// SetData writes the data of a znode if it is at the expected version.
func (c *Client) SetData(path string, data []byte, version int32) (*zk.Stat, error) {
	r, err := c.roundTrip(&zk.Request{Op: zk.OpSetData, Path: path, Data: slices.Clone(data), Version: version}, noWatch, nil)
	if err != nil {
		return nil, err
	}
	return r.Stat, nil
}

// SetDataAsync is the asynchronous form of SetData.
func (c *Client) SetDataAsync(path string, data []byte, version int32, cb func(*zk.Stat, error)) {
	c.do(&zk.Request{Op: zk.OpSetData, Path: path, Data: slices.Clone(data), Version: version}, noWatch, nil,
		func(r *zk.Response, err error) {
			if err != nil {
				cb(nil, err)
				return
			}
			cb(r.Stat, nil)
		})
}

// GetChildren returns the sorted names of the children of a znode.
func (c *Client) GetChildren(path string, watch bool) ([]string, error) {
	r, err := c.roundTrip(&zk.Request{Op: zk.OpGetChildren, Path: path, Watch: watch}, watchChild, c.watcherFor(watch))
	if err != nil {
		return nil, err
	}
	return r.Children, nil
}

// GetChildrenW is GetChildren with a one-time watch on the returned channel.
func (c *Client) GetChildrenW(path string) ([]string, <-chan zk.Event, error) {
	w := newChanWatcher()
	r, err := c.roundTrip(&zk.Request{Op: zk.OpGetChildren, Path: path, Watch: true}, watchChild, w)
	if err != nil {
		return nil, nil, err
	}
	return r.Children, w.ch, nil
}

// GetChildrenAsync is the asynchronous form of GetChildren.
func (c *Client) GetChildrenAsync(path string, watch bool, cb func([]string, error)) {
	c.do(&zk.Request{Op: zk.OpGetChildren, Path: path, Watch: watch}, watchChild, c.watcherFor(watch),
		func(r *zk.Response, err error) {
			if err != nil {
				cb(nil, err)
				return
			}
			cb(r.Children, nil)
		})
}

// Sync waits until every update committed before the call has reached the
// server the client is connected to. A Sync followed by a read is a "slow
// read" that observes the latest state (Section 2.3).
func (c *Client) Sync(path string) error {
	_, err := c.roundTrip(&zk.Request{Op: zk.OpSync, Path: path}, noWatch, nil)
	return err
}

// SyncAsync is the asynchronous form of Sync.
func (c *Client) SyncAsync(path string, cb func(error)) {
	c.do(&zk.Request{Op: zk.OpSync, Path: path}, noWatch, nil, func(_ *zk.Response, err error) { cb(err) })
}

// dispatcher runs callbacks one at a time, in submission order.
type dispatcher struct {
	mu      sync.Mutex
	cond    *sync.Cond
	q       []func()
	stopped bool
	done    chan struct{}
}

func newDispatcher() *dispatcher {
	d := &dispatcher{done: make(chan struct{})}
	d.cond = sync.NewCond(&d.mu)
	go d.run()
	return d
}

func (d *dispatcher) push(f func()) {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		f()
		return
	}
	d.q = append(d.q, f)
	d.cond.Signal()
	d.mu.Unlock()
}

func (d *dispatcher) run() {
	defer close(d.done)
	for {
		d.mu.Lock()
		for len(d.q) == 0 && !d.stopped {
			d.cond.Wait()
		}
		if len(d.q) == 0 {
			d.mu.Unlock()
			return
		}
		f := d.q[0]
		d.q[0] = nil
		d.q = d.q[1:]
		d.mu.Unlock()
		f()
	}
}

// stop drains queued callbacks and stops the goroutine.
func (d *dispatcher) stop() {
	d.mu.Lock()
	d.stopped = true
	d.cond.Broadcast()
	d.mu.Unlock()
	<-d.done
}
