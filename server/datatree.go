package server

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sort"

	"zookeeper/zk"
)

// TxnType identifies a transaction. Transactions are idempotent: they carry
// the *resulting* state (new version, new parent cversion, ...) rather than
// the operation, so applying one twice in order is harmless (Section 4.1).
type TxnType int32

const (
	TxnCreate TxnType = iota + 1
	TxnDelete
	TxnSetData
	TxnError
	TxnCreateSession
	TxnCloseSession
)

func (t TxnType) String() string {
	switch t {
	case TxnCreate:
		return "createTXN"
	case TxnDelete:
		return "deleteTXN"
	case TxnSetData:
		return "setDataTXN"
	case TxnError:
		return "errorTXN"
	case TxnCreateSession:
		return "createSessionTXN"
	case TxnCloseSession:
		return "closeSessionTXN"
	}
	return "unknownTXN"
}

// Txn is a state change agreed upon through atomic broadcast.
type Txn struct {
	Zxid int64
	Type TxnType
	Time int64 // leader wall clock, unix ms

	// Origin of the request, used to answer the client.
	ServerID  int64
	SessionID int64
	Xid       int64

	Path           string
	Data           []byte
	Ephemeral      int64   // owner session for TxnCreate
	Version        int32   // new data version for TxnSetData
	ParentCversion int32   // new parent cversion for TxnCreate/TxnDelete
	Err            zk.Code // TxnError
	Timeout        int64   // TxnCreateSession, ms
	Deletes        []DeleteOp
}

// DeleteOp is the removal of an ephemeral znode when its session closes.
type DeleteOp struct {
	Path           string
	ParentCversion int32
}

type znode struct {
	data     []byte
	stat     zk.Stat
	children map[string]struct{}
}

// DataTree is the in-memory database: the hierarchical name space of znodes
// plus the table of live sessions. It is not safe for concurrent use; the
// server guards it with a RWMutex.
type DataTree struct {
	nodes      map[string]*znode
	ephemerals map[int64]map[string]struct{}
	sessions   map[int64]int64 // session id -> timeout ms
	lastZxid   int64
}

// NewDataTree returns a tree containing only the root znode.
func NewDataTree() *DataTree {
	return &DataTree{
		nodes:      map[string]*znode{"/": {children: map[string]struct{}{}}},
		ephemerals: map[int64]map[string]struct{}{},
		sessions:   map[int64]int64{},
	}
}

// LastZxid returns the zxid of the last applied transaction.
func (t *DataTree) LastZxid() int64 { return t.lastZxid }

func (n *znode) statCopy() zk.Stat {
	s := n.stat
	s.DataLength = int32(len(n.data))
	s.NumChildren = int32(len(n.children))
	return s
}

// Stat returns the metadata of path.
func (t *DataTree) Stat(path string) (zk.Stat, bool) {
	n := t.nodes[path]
	if n == nil {
		return zk.Stat{}, false
	}
	return n.statCopy(), true
}

// Get returns a copy of the data and the metadata of path.
func (t *DataTree) Get(path string) ([]byte, zk.Stat, bool) {
	n := t.nodes[path]
	if n == nil {
		return nil, zk.Stat{}, false
	}
	return slices.Clone(n.data), n.statCopy(), true
}

// Children returns the sorted child names of path.
func (t *DataTree) Children(path string) ([]string, zk.Stat, bool) {
	n := t.nodes[path]
	if n == nil {
		return nil, zk.Stat{}, false
	}
	names := make([]string, 0, len(n.children))
	for c := range n.children {
		names = append(names, c)
	}
	sort.Strings(names)
	return names, n.statCopy(), true
}

// EphemeralsOf returns the ephemeral znodes owned by a session, sorted.
func (t *DataTree) EphemeralsOf(session int64) []string {
	var out []string
	for p := range t.ephemerals[session] {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// HasSession reports whether the session is live.
func (t *DataTree) HasSession(id int64) bool {
	_, ok := t.sessions[id]
	return ok
}

// Sessions returns a copy of the session table.
func (t *DataTree) Sessions() map[int64]int64 {
	out := make(map[int64]int64, len(t.sessions))
	for k, v := range t.sessions {
		out[k] = v
	}
	return out
}

// NodeCount returns the number of znodes including the root.
func (t *DataTree) NodeCount() int { return len(t.nodes) }

// trigger records a watch notification produced by applying a transaction.
type trigger struct {
	path string
	typ  zk.EventType
}

type applyResult struct {
	err      zk.Code
	stat     zk.Stat
	triggers []trigger
}

// Apply applies a transaction. Errors are reported but never abort: during
// recovery from a fuzzy snapshot some transactions are re-applied to a state
// that already contains part of their effects (Section 4.3).
func (t *DataTree) Apply(txn *Txn) applyResult {
	var res applyResult
	switch txn.Type {
	case TxnCreate:
		res = t.create(txn.Path, txn.Data, txn.Ephemeral, txn.ParentCversion, txn.Zxid, txn.Time)
	case TxnDelete:
		res = t.delete(txn.Path, txn.ParentCversion, txn.Zxid)
	case TxnSetData:
		res = t.setData(txn.Path, txn.Data, txn.Version, txn.Zxid, txn.Time)
	case TxnError:
		res.err = txn.Err
	case TxnCreateSession:
		t.sessions[txn.SessionID] = txn.Timeout
	case TxnCloseSession:
		for _, d := range txn.Deletes {
			r := t.delete(d.Path, d.ParentCversion, txn.Zxid)
			res.triggers = append(res.triggers, r.triggers...)
		}
		delete(t.sessions, txn.SessionID)
	}
	if txn.Zxid > t.lastZxid {
		t.lastZxid = txn.Zxid
	}
	return res
}

func (t *DataTree) create(path string, data []byte, owner int64, parentCversion int32, zxid, now int64) applyResult {
	parent := t.nodes[zk.Parent(path)]
	if parent == nil {
		return applyResult{err: zk.CodeNoNode}
	}
	n := t.nodes[path]
	if n == nil {
		n = &znode{children: map[string]struct{}{}}
		t.nodes[path] = n
	}
	n.data = data
	n.stat = zk.Stat{Czxid: zxid, Mzxid: zxid, Pzxid: zxid, Ctime: now, Mtime: now, EphemeralOwner: owner}
	parent.children[zk.Base(path)] = struct{}{}
	parent.stat.Cversion = parentCversion
	parent.stat.Pzxid = zxid
	if owner != 0 {
		set := t.ephemerals[owner]
		if set == nil {
			set = map[string]struct{}{}
			t.ephemerals[owner] = set
		}
		set[path] = struct{}{}
	}
	return applyResult{stat: n.statCopy(), triggers: []trigger{
		{path, zk.EventNodeCreated},
		{zk.Parent(path), zk.EventNodeChildrenChanged},
	}}
}

func (t *DataTree) delete(path string, parentCversion int32, zxid int64) applyResult {
	n := t.nodes[path]
	if n == nil || path == "/" {
		return applyResult{err: zk.CodeNoNode}
	}
	delete(t.nodes, path)
	if owner := n.stat.EphemeralOwner; owner != 0 {
		if set := t.ephemerals[owner]; set != nil {
			delete(set, path)
			if len(set) == 0 {
				delete(t.ephemerals, owner)
			}
		}
	}
	if parent := t.nodes[zk.Parent(path)]; parent != nil {
		delete(parent.children, zk.Base(path))
		parent.stat.Cversion = parentCversion
		parent.stat.Pzxid = zxid
	}
	return applyResult{triggers: []trigger{
		{path, zk.EventNodeDeleted},
		{zk.Parent(path), zk.EventNodeChildrenChanged},
	}}
}

func (t *DataTree) setData(path string, data []byte, version int32, zxid, now int64) applyResult {
	n := t.nodes[path]
	if n == nil {
		return applyResult{err: zk.CodeNoNode}
	}
	n.data = data
	n.stat.Version = version
	n.stat.Mzxid = zxid
	n.stat.Mtime = now
	return applyResult{stat: n.statCopy(), triggers: []trigger{{path, zk.EventNodeDataChanged}}}
}

// Snapshot is a serialized data tree. Zxid is the last zxid applied when the
// snapshot started; for a fuzzy snapshot the content may additionally
// reflect some later transactions.
type Snapshot struct {
	Zxid     int64
	Nodes    []SnapNode
	Sessions map[int64]int64
}

// SnapNode is one znode in a snapshot.
type SnapNode struct {
	Path string
	Data []byte
	Stat zk.Stat
}

// ConsistentSnapshot serializes the whole tree. The caller must prevent
// concurrent modification.
func (t *DataTree) ConsistentSnapshot() *Snapshot {
	snap := &Snapshot{Zxid: t.lastZxid, Sessions: t.Sessions()}
	stack := []string{"/"}
	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n := t.nodes[p]
		if n == nil {
			continue
		}
		snap.Nodes = append(snap.Nodes, SnapNode{Path: p, Data: n.data, Stat: n.stat})
		kids, _, _ := t.Children(p)
		for i := len(kids) - 1; i >= 0; i-- {
			stack = append(stack, zk.Join(p, kids[i]))
		}
	}
	return snap
}

// TreeFromSnapshot rebuilds a data tree. Nodes are listed parents first.
func TreeFromSnapshot(s *Snapshot) *DataTree {
	t := NewDataTree()
	for _, sn := range s.Nodes {
		if sn.Path == "/" {
			t.nodes["/"].stat = sn.Stat
			t.nodes["/"].data = sn.Data
			continue
		}
		parent := t.nodes[zk.Parent(sn.Path)]
		if parent == nil {
			continue // parent removed while a fuzzy snapshot was taken
		}
		t.nodes[sn.Path] = &znode{data: sn.Data, stat: sn.Stat, children: map[string]struct{}{}}
		parent.children[zk.Base(sn.Path)] = struct{}{}
		if o := sn.Stat.EphemeralOwner; o != 0 {
			if t.ephemerals[o] == nil {
				t.ephemerals[o] = map[string]struct{}{}
			}
			t.ephemerals[o][sn.Path] = struct{}{}
		}
	}
	for k, v := range s.Sessions {
		t.sessions[k] = v
	}
	t.lastZxid = s.Zxid
	return t
}

// Digest returns a hash of the tree content (paths, data, versions and
// sessions). Replicas that applied the same transactions have equal digests.
func (t *DataTree) Digest() [32]byte {
	h := sha256.New()
	paths := make([]string, 0, len(t.nodes))
	for p := range t.nodes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var buf [8]byte
	for _, p := range paths {
		n := t.nodes[p]
		h.Write([]byte(p))
		h.Write([]byte{0})
		h.Write(n.data)
		binary.BigEndian.PutUint64(buf[:], uint64(n.stat.Version)<<32|uint64(uint32(n.stat.Cversion)))
		h.Write(buf[:])
		binary.BigEndian.PutUint64(buf[:], uint64(n.stat.EphemeralOwner))
		h.Write(buf[:])
	}
	ids := make([]int64, 0, len(t.sessions))
	for id := range t.sessions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		binary.BigEndian.PutUint64(buf[:], uint64(id))
		h.Write(buf[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
