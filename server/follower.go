package server

import (
	"time"

	"zookeeper/transport"
	"zookeeper/zk"
)

// follow runs the follower role: connect to the leader, take part in
// discovery and synchronization, then log proposals, acknowledge them and
// apply commits in order.
func (s *Server) follow(leaderID int64) {
	c, err := s.cfg.Transport.Dial(s.cfg.Peers[leaderID], s.initTimeout())
	if err != nil {
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
		return
	}
	s.mu.Lock()
	if li.Epoch < s.acceptedEpoch {
		s.mu.Unlock()
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
			s.logf("follower: lost leader %d", leaderID)
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
			s.logf("follower: %v", err)
			return false
		}

	case *SyncSnap:
		s.mu.Lock()
		err := s.installSnapshot(m.Snap, m.Entries)
		s.mu.Unlock()
		if err != nil {
			s.logf("follower: installing snapshot: %v", err)
			return false
		}

	case *NewLeader:
		s.mu.Lock()
		s.currentEpoch = m.Epoch
		s.acceptedEpoch = max(s.acceptedEpoch, m.Epoch)
		s.saveEpochs()
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

	case *Proposal:
		s.mu.Lock()
		z, last := m.Txn.Zxid, s.lastLogged
		inOrder := (epochOf(z) == epochOf(last) && counterOf(z) == counterOf(last)+1) ||
			(epochOf(z) > epochOf(last) && counterOf(z) == 1)
		if !inOrder {
			s.mu.Unlock()
			s.logf("follower: out of order proposal %x after %x", z, last)
			return false
		}
		err := s.appendLog(m.Txn)
		s.mu.Unlock()
		if err != nil {
			s.logf("follower: %v", err)
			return false
		}
		c.Send(&Ack{Zxid: z})

	case *Commit:
		s.mu.Lock()
		if m.Zxid > s.lastLogged {
			s.mu.Unlock()
			return false
		}
		s.applyCommitted(m.Zxid)
		s.mu.Unlock()

	case *LeaderPing:
		c.Send(&PingReply{Sessions: s.drainTouched()})

	case *SyncReply:
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
