package server

import (
	"time"

	"zookeeper/transport"
	"zookeeper/zk"
	"zookeeper/zlog"
)

// follow runs the follower role: connect to the leader, take part in
// discovery and synchronization, then log proposals, acknowledge them and
// apply commits in order.
func (s *Server) follow(leaderID int64) {
	s.lg.Info("★ FOLLOWING: connecting to leader", "leader", leaderID, "addr", s.cfg.Peers[leaderID])
	c, err := s.cfg.Transport.Dial(s.cfg.Peers[leaderID], s.initTimeout())
	if err != nil {
		s.lg.Warn("cannot reach leader", "leader", leaderID, zlog.Err(err))
		s.pause(s.cfg.TickTime)
		return
	}
	if !s.trackConn(c) {
		return
	}
	defer s.untrackConn(c)
	done := make(chan struct{})
	defer close(done)
	in := reader(c, done)
	defer func() {
		s.mu.Lock()
		s.follower = nil
		s.mu.Unlock()
		s.serving.Store(false)
	}()

	s.mu.Lock()
	info := &FollowerInfo{ID: s.id, AcceptedEpoch: s.acceptedEpoch, LastLogged: s.lastLogged}
	s.mu.Unlock()
	c.Send(info)

	m, ok := recvTimeout(in, s.initTimeout(), s.quit)
	li, isInfo := m.(*LeaderInfo)
	if !ok || !isInfo {
		s.lg.Warn("leader did not answer with LEADERINFO in time", "leader", leaderID)
		return
	}
	s.lg.Info("discovery: leader proposes epoch", "epoch", li.Epoch)
	s.mu.Lock()
	if li.Epoch < s.acceptedEpoch {
		s.mu.Unlock()
		s.lg.Warn("rejecting stale epoch", "epoch", li.Epoch, "acceptedEpoch", s.acceptedEpoch)
		return
	}
	if li.Epoch > s.acceptedEpoch {
		s.acceptedEpoch = li.Epoch
		s.saveEpochs()
	}
	ack := &AckEpoch{CurrentEpoch: s.currentEpoch, LastLogged: s.lastLogged}
	s.mu.Unlock()
	c.Send(ack)

	synced := false
	for {
		timeout := s.initTimeout()
		if synced {
			timeout = 2 * s.syncTimeout()
		}
		m, ok := recvTimeout(in, timeout, s.quit)
		if !ok {
			s.lg.Warn("★ lost the leader (no message within timeout or connection closed)", "leader", leaderID, "timeout", timeout)
			return
		}
		if !s.followerHandle(c, m, &synced) {
			return
		}
	}
}

func (s *Server) followerHandle(c transport.Conn, m any, synced *bool) bool {
	switch m := m.(type) {
	case *SyncDiff:
		s.mu.Lock()
		var fresh []*Txn
		for _, t := range m.Entries {
			if t.Zxid > s.lastLogged {
				fresh = append(fresh, t)
			}
		}
		err := s.appendLog(fresh...)
		s.mu.Unlock()
		if err != nil {
			s.lg.Error("logging DIFF failed", zlog.Err(err))
			return false
		}
		s.lg.Info("synchronization: received DIFF", "txns", len(fresh))

	case *SyncSnap:
		s.mu.Lock()
		err := s.installSnapshot(m.Snap, m.Entries)
		s.mu.Unlock()
		if err != nil {
			s.lg.Error("installing snapshot failed", zlog.Err(err))
			return false
		}

	case *NewLeader:
		s.mu.Lock()
		s.currentEpoch = m.Epoch
		s.acceptedEpoch = max(s.acceptedEpoch, m.Epoch)
		s.saveEpochs()
		s.lg.Info("NEWLEADER received: adopting epoch and committing up to", "epoch", m.Epoch,
			"commit", zlog.ZxidString(m.CommitZxid), "from", zlog.ZxidString(s.lastCommitted))
		s.applyCommitted(m.CommitZxid)
		var inflight []int64
		for i := s.logIndexAfter(m.CommitZxid); i < len(s.log); i++ {
			inflight = append(inflight, s.log[i].Zxid)
		}
		s.mu.Unlock()
		c.Send(&AckNewLeader{Epoch: m.Epoch})
		for _, z := range inflight {
			c.Send(&Ack{Zxid: z})
		}

	case *UpToDate:
		s.mu.Lock()
		s.follower = c
		s.mu.Unlock()
		s.serving.Store(true)
		*synced = true
		s.lg.Info("★ UPTODATE: synchronized with leader, serving clients", "lastZxid", zlog.ZxidString(s.LastZxid()))

	case *Proposal:
		s.mu.Lock()
		z, last := m.Txn.Zxid, s.lastLogged
		inOrder := (epochOf(z) == epochOf(last) && counterOf(z) == counterOf(last)+1) ||
			(epochOf(z) > epochOf(last) && counterOf(z) == 1)
		if !inOrder {
			s.mu.Unlock()
			s.lg.Warn("out of order proposal, resynchronizing", "got", zlog.ZxidString(z), "last", zlog.ZxidString(last))
			return false
		}
		err := s.appendLog(m.Txn)
		s.mu.Unlock()
		if err != nil {
			s.lg.Error("log append failed", zlog.Err(err))
			return false
		}
		s.lg.Debug("PROPOSAL logged to disk, sending ACK", txnAttrs(m.Txn)...)
		c.Send(&Ack{Zxid: z})

	case *Commit:
		s.mu.Lock()
		if m.Zxid > s.lastLogged {
			s.mu.Unlock()
			return false
		}
		s.lg.Debug("COMMIT received", zlog.Zxid(m.Zxid))
		s.applyCommitted(m.Zxid)
		s.mu.Unlock()

	case *LeaderPing:
		s.trace("heartbeat from leader", "leaderCommitted", zlog.ZxidString(m.Zxid))
		c.Send(&PingReply{Sessions: s.drainTouched()})

	case *SyncReply:
		s.lg.Debug("sync completed by leader", zlog.Session(m.SessionID))
		s.completeLocal(m.SessionID, m.Xid, &zk.Response{Xid: m.Xid, Zxid: s.LastZxid()})
	}
	return true
}

// pause sleeps unless the server is stopping.
func (s *Server) pause(d time.Duration) {
	select {
	case <-time.After(d):
	case <-s.quit:
	}
}
