// Package zk contains the types shared by the ZooKeeper client and server:
// znode metadata, create flags, watch events, error codes and the wire
// protocol spoken between clients and servers.
//
// The design follows "ZooKeeper: Wait-free coordination for Internet-scale
// systems" (Hunt, Konar, Junqueira, Reed — USENIX ATC 2010).
package zk

import (
	"errors"
	"fmt"
	"strings"
)

// CreateFlag selects the znode type on create (Section 2.1).
type CreateFlag int32

const (
	// FlagPersistent creates a regular znode, removed only by an explicit delete.
	FlagPersistent CreateFlag = 0
	// FlagEphemeral creates a znode that is removed automatically when the
	// session that created it ends.
	FlagEphemeral CreateFlag = 1
	// FlagSequential appends a monotonically increasing counter to the name.
	FlagSequential CreateFlag = 2
)

// AnyVersion disables the version check of conditional updates.
const AnyVersion int32 = -1

// MaxDataSize is the maximum amount of data a znode can hold (1MB, Section 4).
const MaxDataSize = 1 << 20

// SequenceDigits is the width of the counter appended to sequential znodes.
const SequenceDigits = 10

// Stat is the meta-data associated with each znode.
type Stat struct {
	Czxid          int64 // zxid of the change that created the znode
	Mzxid          int64 // zxid of the last data change
	Pzxid          int64 // zxid of the last change to the children
	Ctime          int64 // creation time (unix ms)
	Mtime          int64 // last modification time (unix ms)
	Version        int32 // number of data changes
	Cversion       int32 // number of children changes
	EphemeralOwner int64 // session id of the owner, 0 for regular znodes
	DataLength     int32
	NumChildren    int32
}

// EventType identifies the kind of watch notification.
type EventType int32

const (
	// EventSession is a session state change (connection loss, expiry...).
	EventSession EventType = iota
	EventNodeCreated
	EventNodeDeleted
	EventNodeDataChanged
	EventNodeChildrenChanged
)

func (t EventType) String() string {
	switch t {
	case EventSession:
		return "Session"
	case EventNodeCreated:
		return "NodeCreated"
	case EventNodeDeleted:
		return "NodeDeleted"
	case EventNodeDataChanged:
		return "NodeDataChanged"
	case EventNodeChildrenChanged:
		return "NodeChildrenChanged"
	}
	return fmt.Sprintf("EventType(%d)", int32(t))
}

// State is the state of a client session.
type State int32

const (
	StateDisconnected State = iota
	StateConnecting
	StateConnected
	StateExpired
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "Disconnected"
	case StateConnecting:
		return "Connecting"
	case StateConnected:
		return "Connected"
	case StateExpired:
		return "Expired"
	case StateClosed:
		return "Closed"
	}
	return fmt.Sprintf("State(%d)", int32(s))
}

// Event is delivered to watchers. Watch events only indicate that a change
// happened; they do not carry the new state (Section 2.1).
type Event struct {
	Type  EventType
	State State
	Path  string
	Err   error
}

func (e Event) String() string {
	if e.Type == EventSession {
		return fmt.Sprintf("Event{Session %v}", e.State)
	}
	return fmt.Sprintf("Event{%v %s}", e.Type, e.Path)
}

// Code is the wire representation of an error.
type Code int32

const (
	CodeOK Code = iota
	CodeNoNode
	CodeNodeExists
	CodeBadVersion
	CodeNotEmpty
	CodeNoChildrenForEphemerals
	CodeBadArguments
	CodeSessionExpired
	CodeConnectionLoss
	CodeNotReady
	CodeServerBehind
	CodeClosed
	CodeSystemError
)

var (
	ErrNoNode                  = errors.New("zk: node does not exist")
	ErrNodeExists              = errors.New("zk: node already exists")
	ErrBadVersion              = errors.New("zk: version conflict")
	ErrNotEmpty                = errors.New("zk: node has children")
	ErrNoChildrenForEphemerals = errors.New("zk: ephemeral nodes may not have children")
	ErrBadArguments            = errors.New("zk: invalid arguments")
	ErrSessionExpired          = errors.New("zk: session has expired")
	ErrConnectionLoss          = errors.New("zk: connection to server lost")
	ErrNotReady                = errors.New("zk: server is not serving requests")
	ErrServerBehind            = errors.New("zk: server is behind the client's view")
	ErrClosed                  = errors.New("zk: client or server closed")
	ErrSystemError             = errors.New("zk: system error")
)

var codeToErr = map[Code]error{
	CodeNoNode:                  ErrNoNode,
	CodeNodeExists:              ErrNodeExists,
	CodeBadVersion:              ErrBadVersion,
	CodeNotEmpty:                ErrNotEmpty,
	CodeNoChildrenForEphemerals: ErrNoChildrenForEphemerals,
	CodeBadArguments:            ErrBadArguments,
	CodeSessionExpired:          ErrSessionExpired,
	CodeConnectionLoss:          ErrConnectionLoss,
	CodeNotReady:                ErrNotReady,
	CodeServerBehind:            ErrServerBehind,
	CodeClosed:                  ErrClosed,
	CodeSystemError:             ErrSystemError,
}

// Err converts a code into an error (nil for CodeOK).
func (c Code) Err() error {
	if c == CodeOK {
		return nil
	}
	if err, ok := codeToErr[c]; ok {
		return err
	}
	return fmt.Errorf("zk: unknown error code %d", int32(c))
}

// CodeOf converts an error into its wire code.
func CodeOf(err error) Code {
	if err == nil {
		return CodeOK
	}
	for c, e := range codeToErr {
		if errors.Is(err, e) {
			return c
		}
	}
	return CodeSystemError
}

// ValidatePath checks that p is an absolute, canonical znode path.
func ValidatePath(p string) error {
	if p == "" || p[0] != '/' {
		return ErrBadArguments
	}
	if p == "/" {
		return nil
	}
	if strings.HasSuffix(p, "/") {
		return ErrBadArguments
	}
	for _, part := range strings.Split(p[1:], "/") {
		if part == "" || part == "." || part == ".." {
			return ErrBadArguments
		}
		if strings.ContainsRune(part, 0) {
			return ErrBadArguments
		}
	}
	return nil
}

// Parent returns the parent path of p ("/" for top-level znodes).
func Parent(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i <= 0 {
		return "/"
	}
	return p[:i]
}

// Base returns the last component of p.
func Base(p string) string {
	return p[strings.LastIndexByte(p, '/')+1:]
}

// Join joins a parent path and a child name.
func Join(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

// SequenceOf extracts the counter appended to a sequential znode name.
func SequenceOf(name string) (int64, bool) {
	if len(name) < SequenceDigits {
		return 0, false
	}
	var n int64
	for _, ch := range name[len(name)-SequenceDigits:] {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		n = n*10 + int64(ch-'0')
	}
	return n, true
}
