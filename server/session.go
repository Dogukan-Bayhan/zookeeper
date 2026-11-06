package server

import (
	"math/rand/v2"
	"sync"
	"time"

	"zookeeper/transport"
	"zookeeper/zk"
)

// clientSession is a session connected to this server. Requests of a session
// are answered strictly in the order they were sent (FIFO client order,
// Section 2.3): writes are pipelined through the leader, and a read waits
// until every earlier request of the session has completed.
type clientSession struct {
	srv     *Server
	id      int64
	timeout int64
	conn    transport.Conn

	mu      sync.Mutex
	pending []*pendingOp
	closed  bool
}

type pendingOp struct {
	req   *zk.Request
	write bool // goes through the leader (writes and sync)
	done  bool
	resp  *zk.Response
}

func (cs *clientSession) send(msg any) { cs.conn.Send(msg) }

func (cs *clientSession) submit(r *zk.Request) {
	op := &pendingOp{req: r, write: r.Op.IsWrite() || r.Op == zk.OpSync}
	cs.mu.Lock()
	if cs.closed {
		cs.mu.Unlock()
		return
	}
	cs.pending = append(cs.pending, op)
	if !op.write {
		cs.flushLocked()
		cs.mu.Unlock()
		return
	}
	cs.mu.Unlock()

	var err error
	switch r.Op {
	case zk.OpCreateSession:
		cs.complete(r.Xid, &zk.Response{Xid: r.Xid, Err: zk.CodeBadArguments})
		return
	case zk.OpSync:
		err = cs.srv.submitSync(cs.id, r.Xid)
	default:
		err = cs.srv.submitWrite(&Forward{ServerID: cs.srv.id, SessionID: cs.id, Req: r})
	}
	if err != nil {
		// We lost the leader; the client will reconnect elsewhere.
		cs.conn.Close()
	}
}

// complete records the response of a write (or sync) and flushes every
// response that is now at the head of the session queue.
func (cs *clientSession) complete(xid int64, resp *zk.Response) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, op := range cs.pending {
		if op.write && !op.done && op.req.Xid == xid {
			op.done = true
			op.resp = resp
			break
		}
	}
	cs.flushLocked()
}

func (cs *clientSession) flushLocked() {
	for len(cs.pending) > 0 && !cs.closed {
		op := cs.pending[0]
		if op.write {
			if !op.done {
				return
			}
			cs.conn.Send(op.resp)
		} else {
			cs.srv.execRead(cs, op.req)
		}
		cs.pending[0] = nil
		cs.pending = cs.pending[1:]
	}
}

func (cs *clientSession) close() {
	cs.mu.Lock()
	cs.closed = true
	cs.pending = nil
	cs.mu.Unlock()
	cs.conn.Close()
	cs.srv.watches.removeSession(cs)
}

// execRead serves a read from the local replica (Section 4.4). The response
// is queued while holding the read lock so that it is ordered with respect
// to watch notifications queued by concurrent writes.
func (s *Server) execRead(cs *clientSession, r *zk.Request) {
	resp := &zk.Response{Xid: r.Xid}
	if err := zk.ValidatePath(r.Path); err != nil {
		resp.Err = zk.CodeBadArguments
		cs.conn.Send(resp)
		return
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	resp.Zxid = s.tree.lastZxid
	switch r.Op {
	case zk.OpExists:
		st, ok := s.tree.Stat(r.Path)
		if r.Watch {
			// An exists watch on a missing node fires when it is created.
			s.watches.addData(r.Path, cs)
		}
		if ok {
			resp.Stat = &st
		} else {
			resp.Err = zk.CodeNoNode
		}
	case zk.OpGetData:
		data, st, ok := s.tree.Get(r.Path)
		if !ok {
			resp.Err = zk.CodeNoNode
			break
		}
		if r.Watch {
			s.watches.addData(r.Path, cs)
		}
		resp.Data, resp.Stat = data, &st
	case zk.OpGetChildren:
		kids, st, ok := s.tree.Children(r.Path)
		if !ok {
			resp.Err = zk.CodeNoNode
			break
		}
		if r.Watch {
			s.watches.addChild(r.Path, cs)
		}
		resp.Children, resp.Stat = kids, &st
	default:
		resp.Err = zk.CodeBadArguments
	}
	cs.conn.Send(resp)
}

func (s *Server) clampTimeout(ms int64) int64 {
	lo := s.cfg.MinSessionTimeout.Milliseconds()
	hi := s.cfg.MaxSessionTimeout.Milliseconds()
	return max(lo, min(hi, ms))
}

func (s *Server) newSessionID() int64 {
	return (s.id&0x7f)<<56 | rand.Int64N(1<<56)
}

// handleClient runs a client connection: session establishment, then the
// request loop.
func (s *Server) handleClient(conn transport.Conn, req *zk.ConnectRequest) {
	defer conn.Close()
	if !s.serving.Load() {
		conn.Send(&zk.ConnectResponse{Err: zk.CodeNotReady})
		return
	}
	timeout := s.clampTimeout(req.Timeout)
	var cs *clientSession
	if req.SessionID == 0 {
		id := s.newSessionID()
		cs = &clientSession{srv: s, id: id, timeout: timeout, conn: conn}
		ch := make(chan zk.Code, 1)
		s.smu.Lock()
		s.connects[id] = ch
		s.smu.Unlock()
		defer func() {
			s.smu.Lock()
			delete(s.connects, id)
			s.smu.Unlock()
		}()
		fwd := &Forward{ServerID: s.id, SessionID: id, Timeout: timeout, Req: &zk.Request{Op: zk.OpCreateSession}}
		if err := s.submitWrite(fwd); err != nil {
			conn.Send(&zk.ConnectResponse{Err: zk.CodeNotReady})
			return
		}
		wait := time.Duration(timeout)*time.Millisecond + s.cfg.TickTime*time.Duration(s.cfg.InitLimit)
		select {
		case code := <-ch:
			if code != zk.CodeOK {
				conn.Send(&zk.ConnectResponse{Err: code})
				return
			}
		case <-time.After(wait):
			return
		case <-s.quit:
			return
		}
		s.dbMu.RLock()
		old := s.registerSession(cs)
		conn.Send(&zk.ConnectResponse{SessionID: id, Timeout: timeout, Zxid: s.tree.lastZxid})
		s.dbMu.RUnlock()
		if old != nil {
			old.close()
		}
	} else {
		s.dbMu.RLock()
		if req.LastZxid > s.tree.lastZxid {
			// This server has not yet seen state the client has seen: refuse
			// so the client picks a more recent server (Section 4.4).
			s.dbMu.RUnlock()
			conn.Send(&zk.ConnectResponse{Err: zk.CodeServerBehind})
			return
		}
		if !s.tree.HasSession(req.SessionID) {
			s.dbMu.RUnlock()
			conn.Send(&zk.ConnectResponse{Err: zk.CodeSessionExpired})
			return
		}
		cs = &clientSession{srv: s, id: req.SessionID, timeout: timeout, conn: conn}
		old := s.registerSession(cs)
		conn.Send(&zk.ConnectResponse{SessionID: cs.id, Timeout: timeout, Zxid: s.tree.lastZxid})
		s.rewatch(cs, req)
		s.dbMu.RUnlock()
		if old != nil {
			old.close()
		}
	}
	s.touch(cs.id)
	for {
		m, err := conn.Recv()
		if err != nil {
			break
		}
		r, ok := m.(*zk.Request)
		if !ok {
			break
		}
		s.touch(cs.id)
		if r.Op == zk.OpPing {
			conn.Send(&zk.Response{Xid: zk.PingXid, Zxid: s.LastZxid()})
			continue
		}
		cs.submit(r)
	}
	cs.close()
	s.unregisterSession(cs)
}

// rewatch re-registers the watches of a session that moved to this server,
// firing immediately those whose condition changed after lastZxid. Called
// with dbMu held.
func (s *Server) rewatch(cs *clientSession, req *zk.ConnectRequest) {
	rel, zxid := req.LastZxid, s.tree.lastZxid
	fire := func(t zk.EventType, p string) {
		cs.send(&zk.WatcherEvent{Type: t, Path: p, Zxid: zxid})
	}
	for _, p := range req.DataWatches {
		st, ok := s.tree.Stat(p)
		switch {
		case !ok:
			fire(zk.EventNodeDeleted, p)
		case st.Mzxid > rel:
			fire(zk.EventNodeDataChanged, p)
		default:
			s.watches.addData(p, cs)
		}
	}
	for _, p := range req.ExistWatches {
		if _, ok := s.tree.Stat(p); ok {
			fire(zk.EventNodeCreated, p)
		} else {
			s.watches.addData(p, cs)
		}
	}
	for _, p := range req.ChildWatches {
		_, st, ok := s.tree.Children(p)
		switch {
		case !ok:
			fire(zk.EventNodeDeleted, p)
		case st.Pzxid > rel:
			fire(zk.EventNodeChildrenChanged, p)
		default:
			s.watches.addChild(p, cs)
		}
	}
}

// registerSession makes cs the local session for its id and returns the
// session it replaced, which the caller must close (outside dbMu).
func (s *Server) registerSession(cs *clientSession) *clientSession {
	s.smu.Lock()
	defer s.smu.Unlock()
	old := s.sessions[cs.id]
	s.sessions[cs.id] = cs
	return old
}

func (s *Server) unregisterSession(cs *clientSession) {
	s.smu.Lock()
	if s.sessions[cs.id] == cs {
		delete(s.sessions, cs.id)
	}
	s.smu.Unlock()
}

func (s *Server) localSession(id int64) *clientSession {
	s.smu.Lock()
	defer s.smu.Unlock()
	return s.sessions[id]
}

// closeAllClients drops every client connection. Called when the server
// stops serving (leader change): clients reconnect and re-validate.
func (s *Server) closeAllClients() {
	s.smu.Lock()
	all := make([]*clientSession, 0, len(s.sessions))
	for _, cs := range s.sessions {
		all = append(all, cs)
	}
	s.sessions = map[int64]*clientSession{}
	s.smu.Unlock()
	for _, cs := range all {
		cs.close()
	}
	s.watches.clear()
}

// touch records activity of a session; the leader expires sessions from
// which no server heard anything within the session timeout.
func (s *Server) touch(id int64) {
	s.touchMu.Lock()
	s.touched[id] = struct{}{}
	s.touchMu.Unlock()
}

func (s *Server) drainTouched() []int64 {
	s.touchMu.Lock()
	defer s.touchMu.Unlock()
	out := make([]int64, 0, len(s.touched))
	for id := range s.touched {
		out = append(out, id)
	}
	clear(s.touched)
	return out
}

// completeLocal answers a request of a session connected to this server.
func (s *Server) completeLocal(sessionID, xid int64, resp *zk.Response) {
	if cs := s.localSession(sessionID); cs != nil {
		cs.complete(xid, resp)
	}
}
