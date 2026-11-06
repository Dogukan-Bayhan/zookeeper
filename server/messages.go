package server

import (
	"encoding/gob"

	"zookeeper/zk"
)

// PeerState is the role of a server in the ensemble.
type PeerState int32

const (
	Looking PeerState = iota
	Following
	Leading
	Stopped
)

func (s PeerState) String() string {
	switch s {
	case Looking:
		return "LOOKING"
	case Following:
		return "FOLLOWING"
	case Leading:
		return "LEADING"
	case Stopped:
		return "STOPPED"
	}
	return "?"
}

// ---- leader election ----

// ElectionHello opens an election connection.
type ElectionHello struct{ From int64 }

// Notification is a vote exchanged during leader election. Votes are ordered
// by (Epoch, Zxid, Leader): the server with the most recent history wins.
type Notification struct {
	From   int64
	Leader int64
	Zxid   int64
	Epoch  int64
	Round  int64
	State  PeerState
}

// ---- Zab: discovery and synchronization ----

// FollowerInfo opens a follower->leader connection.
type FollowerInfo struct {
	ID            int64
	AcceptedEpoch int64
	LastLogged    int64
}

// LeaderInfo proposes the new epoch.
type LeaderInfo struct{ Epoch int64 }

// AckEpoch accepts the new epoch and reports the follower history.
type AckEpoch struct {
	CurrentEpoch int64
	LastLogged   int64
}

// SyncDiff sends the transactions a follower is missing.
type SyncDiff struct{ Entries []*Txn }

// SyncSnap replaces the follower state with a snapshot of the leader, plus
// the proposals that are still in flight.
type SyncSnap struct {
	Snap    *Snapshot
	Entries []*Txn
}

// NewLeader ends synchronization: the follower adopts the epoch and commits
// its log up to CommitZxid.
type NewLeader struct {
	Epoch      int64
	CommitZxid int64
}

// AckNewLeader acknowledges NewLeader.
type AckNewLeader struct{ Epoch int64 }

// UpToDate tells the follower it may start serving clients.
type UpToDate struct{ Zxid int64 }

// ---- Zab: broadcast ----

// Proposal carries a transaction to be logged by followers.
type Proposal struct{ Txn *Txn }

// Ack acknowledges that a proposal is on stable storage.
type Ack struct{ Zxid int64 }

// Commit tells followers to apply every logged transaction up to Zxid.
type Commit struct{ Zxid int64 }

// LeaderPing is the leader heartbeat.
type LeaderPing struct{ Zxid int64 }

// PingReply answers a heartbeat with the sessions that were active on the
// follower since the previous reply, so that the leader can keep them alive.
type PingReply struct{ Sessions []int64 }

// Forward carries a client write from a follower to the leader.
type Forward struct {
	ServerID  int64
	SessionID int64
	Timeout   int64 // OpCreateSession only
	Req       *zk.Request
}

// SyncRequest asks the leader to flush the channel to the follower (sync).
type SyncRequest struct {
	SessionID int64
	Xid       int64
}

// SyncReply is sent by the leader after every commit that was pending when
// the matching SyncRequest arrived.
type SyncReply struct {
	SessionID int64
	Xid       int64
}

func init() {
	gob.Register(&ElectionHello{})
	gob.Register(&Notification{})
	gob.Register(&FollowerInfo{})
	gob.Register(&LeaderInfo{})
	gob.Register(&AckEpoch{})
	gob.Register(&SyncDiff{})
	gob.Register(&SyncSnap{})
	gob.Register(&NewLeader{})
	gob.Register(&AckNewLeader{})
	gob.Register(&UpToDate{})
	gob.Register(&Proposal{})
	gob.Register(&Ack{})
	gob.Register(&Commit{})
	gob.Register(&LeaderPing{})
	gob.Register(&PingReply{})
	gob.Register(&Forward{})
	gob.Register(&SyncRequest{})
	gob.Register(&SyncReply{})
}
