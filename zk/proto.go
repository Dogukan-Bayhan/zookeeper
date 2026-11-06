package zk

import "encoding/gob"

// OpCode identifies a client request.
type OpCode int32

const (
	OpCreate OpCode = iota + 1
	OpDelete
	OpSetData
	OpExists
	OpGetData
	OpGetChildren
	OpSync
	OpPing
	OpCloseSession
	// OpCreateSession is internal: it is never sent by clients, servers use it
	// to turn a new connection into a replicated session.
	OpCreateSession
)

// IsWrite reports whether the operation changes the replicated state and
// therefore goes through atomic broadcast.
func (o OpCode) IsWrite() bool {
	switch o {
	case OpCreate, OpDelete, OpSetData, OpCloseSession, OpCreateSession:
		return true
	}
	return false
}

func (o OpCode) String() string {
	switch o {
	case OpCreate:
		return "create"
	case OpDelete:
		return "delete"
	case OpSetData:
		return "setData"
	case OpExists:
		return "exists"
	case OpGetData:
		return "getData"
	case OpGetChildren:
		return "getChildren"
	case OpSync:
		return "sync"
	case OpPing:
		return "ping"
	case OpCloseSession:
		return "closeSession"
	case OpCreateSession:
		return "createSession"
	}
	return "unknown"
}

// PingXid is the xid used for heartbeats.
const PingXid int64 = -2

// ConnectRequest is the first message a client sends on a new connection.
// It creates a session (SessionID == 0) or moves an existing session to this
// server. LastZxid is the last zxid the client has seen: a server whose state
// is older refuses the session (Section 4.4). The watch lists let the server
// re-register the client's outstanding watches.
type ConnectRequest struct {
	SessionID    int64
	Timeout      int64 // milliseconds
	LastZxid     int64
	DataWatches  []string
	ExistWatches []string
	ChildWatches []string
}

// ConnectResponse answers a ConnectRequest.
type ConnectResponse struct {
	SessionID int64
	Timeout   int64 // negotiated, milliseconds
	Zxid      int64
	Err       Code
}

// Request is a client operation.
type Request struct {
	Xid     int64
	Op      OpCode
	Path    string
	Data    []byte
	Flags   CreateFlag
	Version int32
	Watch   bool
}

// Response answers a Request. Every response carries the zxid of the server
// state it reflects.
type Response struct {
	Xid      int64
	Zxid     int64
	Err      Code
	Path     string
	Data     []byte
	Stat     *Stat
	Children []string
}

// WatcherEvent is a watch notification pushed by a server.
type WatcherEvent struct {
	Type EventType
	Path string
	Zxid int64
}

func init() {
	gob.Register(&ConnectRequest{})
	gob.Register(&ConnectResponse{})
	gob.Register(&Request{})
	gob.Register(&Response{})
	gob.Register(&WatcherEvent{})
}
