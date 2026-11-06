package server

import (
	"time"

	"zookeeper/transport"
)

// Leader election. The details of leader election are outside the scope of
// the paper; we use a simple round-based voting protocol in the spirit of
// ZooKeeper's fast leader election: every LOOKING server proposes the server
// with the most recent history, adopts better proposals it hears about, and
// settles once a quorum agrees on the same vote. A server that is already
// part of an established ensemble answers lookers with its leader, so a
// recovering server simply joins.

type vote struct {
	leader, zxid, epoch int64
}

// better orders votes by (epoch, zxid, server id).
func better(a, b vote) bool {
	if a.epoch != b.epoch {
		return a.epoch > b.epoch
	}
	if a.zxid != b.zxid {
		return a.zxid > b.zxid
	}
	return a.leader > b.leader
}

func (n *Notification) vote() vote {
	return vote{leader: n.Leader, zxid: n.Zxid, epoch: n.Epoch}
}

// peerSender delivers election messages to one peer over a lazily dialed
// connection. Messages may be lost; election rebroadcasts periodically.
type peerSender struct {
	ch chan any
}

func (s *Server) sendPeer(id int64, msg any) {
	s.peerMu.Lock()
	select {
	case <-s.quit:
		s.peerMu.Unlock()
		return
	default:
	}
	ps := s.peerOut[id]
	if ps == nil {
		ps = &peerSender{ch: make(chan any, 256)}
		s.peerOut[id] = ps
		s.wg.Add(1)
		go s.peerSendLoop(id, ps)
	}
	s.peerMu.Unlock()
	select {
	case ps.ch <- msg:
	default: // congested: drop
	}
}

func (s *Server) peerSendLoop(id int64, ps *peerSender) {
	defer s.wg.Done()
	var c transport.Conn
	defer func() {
		if c != nil {
			s.untrackConn(c)
		}
	}()
	for {
		var msg any
		select {
		case <-s.quit:
			return
		case msg = <-ps.ch:
		}
		if c == nil {
			conn, err := s.cfg.Transport.Dial(s.cfg.Peers[id], s.cfg.TickTime*5)
			if err != nil {
				continue
			}
			if !s.trackConn(conn) {
				return
			}
			if conn.Send(&ElectionHello{From: s.id}) != nil {
				s.untrackConn(conn)
				continue
			}
			c = conn
		}
		if err := c.Send(msg); err != nil {
			s.untrackConn(c)
			c = nil
		}
	}
}

func (s *Server) readElection(c transport.Conn) {
	for {
		m, err := c.Recv()
		if err != nil {
			return
		}
		if n, ok := m.(*Notification); ok {
			s.handleNotification(n)
		}
	}
}

func (s *Server) handleNotification(n *Notification) {
	s.mu.Lock()
	state := s.state
	if state == Looking {
		s.mu.Unlock()
		select {
		case s.electionIn <- n:
		default:
		}
		return
	}
	if n.State != Looking || (state != Following && state != Leading) {
		s.mu.Unlock()
		return
	}
	// Tell the looking server who leads the ensemble we belong to.
	reply := &Notification{
		From: s.id, Leader: s.leaderID, Zxid: s.lastLogged,
		Epoch: s.currentEpoch, Round: n.Round, State: state,
	}
	s.mu.Unlock()
	s.sendPeer(n.From, reply)
}

// lookForLeader runs an election and returns the elected leader.
func (s *Server) lookForLeader() (int64, bool) {
	s.mu.Lock()
	s.round++
	round := s.round
	self := vote{leader: s.id, zxid: s.lastLogged, epoch: s.currentEpoch}
	s.mu.Unlock()
	if len(s.cfg.Peers) == 1 {
		return s.id, true
	}
	// Drop notifications from previous elections; live peers rebroadcast.
	for drained := false; !drained; {
		select {
		case <-s.electionIn:
		default:
			drained = true
		}
	}

	cur := self
	votes := map[int64]vote{}           // LOOKING peers, current round
	others := map[int64]*Notification{} // peers in an established ensemble
	notify := func() {
		for id := range s.cfg.Peers {
			if id != s.id {
				s.sendPeer(id, &Notification{From: s.id, Leader: cur.leader, Zxid: cur.zxid,
					Epoch: cur.epoch, Round: round, State: Looking})
			}
		}
	}
	notify()
	ticker := time.NewTicker(2 * s.cfg.TickTime)
	defer ticker.Stop()
	finalize := 2 * s.cfg.TickTime
	var pending []*Notification

	for {
		var n *Notification
		if len(pending) > 0 {
			n, pending = pending[0], pending[1:]
		} else {
			select {
			case <-s.quit:
				return 0, false
			case <-ticker.C:
				notify()
				continue
			case n = <-s.electionIn:
			}
		}
		if n.From == s.id {
			continue
		}

		if n.State != Looking {
			delete(votes, n.From)
			others[n.From] = n
			l := n.Leader
			if ln, ok := others[l]; (ok && ln.State == Leading) || l == s.id {
				count := 1 // ourselves
				for _, o := range others {
					if o.Leader == l {
						count++
					}
				}
				if count >= s.quorum && l != s.id {
					return l, true
				}
			}
			continue
		}

		delete(others, n.From)
		switch {
		case n.Round > round:
			round = n.Round
			s.mu.Lock()
			s.round = round
			s.mu.Unlock()
			clear(votes)
			cur = self
			if better(n.vote(), cur) {
				cur = n.vote()
			}
			notify()
		case n.Round < round:
			// Help a lagging server catch up with the current round.
			s.sendPeer(n.From, &Notification{From: s.id, Leader: cur.leader, Zxid: cur.zxid,
				Epoch: cur.epoch, Round: round, State: Looking})
			continue
		default:
			if better(n.vote(), cur) {
				cur = n.vote()
				notify()
			}
		}
		votes[n.From] = n.vote()
		votes[s.id] = cur

		agree := 0
		for _, v := range votes {
			if v == cur {
				agree++
			}
		}
		if agree < s.quorum {
			continue
		}
		// A quorum agrees: wait briefly for a better vote before deciding.
		improved := false
		deadline := time.NewTimer(finalize)
	wait:
		for {
			select {
			case m := <-s.electionIn:
				pending = append(pending, m)
				if m.State == Looking && m.Round == round && better(m.vote(), cur) {
					improved = true
					break wait
				}
			case <-deadline.C:
				break wait
			case <-s.quit:
				deadline.Stop()
				return 0, false
			}
		}
		deadline.Stop()
		if !improved {
			return cur.leader, true
		}
	}
}
