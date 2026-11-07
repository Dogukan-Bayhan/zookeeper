package server

// Stats is a point-in-time view of a server, used by monitoring tools such
// as the web lab.
type Stats struct {
	ID            int64
	State         PeerState
	Leader        int64 // 0 while looking
	Serving       bool
	AcceptedEpoch int64
	CurrentEpoch  int64
	LastLogged    int64
	LastCommitted int64
	LastApplied   int64 // zxid of the local replica
	Znodes        int
	Sessions      int // live sessions in the replicated session table
	LocalClients  int // sessions connected to this server
	Followers     int // synchronized followers (leader only)
	Digest        [32]byte
}

// Stats returns the current state of the server.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	st := Stats{
		ID: s.id, State: s.state, AcceptedEpoch: s.acceptedEpoch, CurrentEpoch: s.currentEpoch,
		LastLogged: s.lastLogged, LastCommitted: s.lastCommitted,
	}
	if s.state != Looking {
		st.Leader = s.leaderID
	}
	if s.leader != nil {
		st.Followers = len(s.leader.forwarding)
	}
	s.mu.Unlock()
	st.Serving = s.serving.Load()

	s.dbMu.RLock()
	st.LastApplied = s.tree.lastZxid
	st.Znodes = s.tree.NodeCount()
	st.Sessions = len(s.tree.sessions)
	st.Digest = s.tree.Digest()
	s.dbMu.RUnlock()

	s.smu.Lock()
	st.LocalClients = len(s.sessions)
	s.smu.Unlock()
	return st
}

// Snapshot returns a consistent copy of the local replica of the data tree.
// Since reads are served locally, replicas may briefly differ.
func (s *Server) Snapshot() *Snapshot {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.tree.ConsistentSnapshot()
}
