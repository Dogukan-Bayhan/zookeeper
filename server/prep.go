package server

import (
	"fmt"
	"sort"
	"time"

	"zookeeper/zk"
)

// Request processor (Section 4.1). When the leader receives a write it
// computes the state the system will be in once the write is applied and
// turns the request into an idempotent transaction capturing that state.
// The future state must take into account outstanding transactions that
// have not been applied yet; it is tracked with change records layered over
// the committed data tree.

type changeRecord struct {
	zxid   int64 // last outstanding txn touching the record
	exists bool
	stat   zk.Stat
}

// record returns the future state of path.
func (ls *leaderState) record(path string) *changeRecord {
	if r, ok := ls.records[path]; ok {
		return r
	}
	ls.s.dbMu.RLock()
	st, ok := ls.s.tree.Stat(path)
	ls.s.dbMu.RUnlock()
	return &changeRecord{exists: ok, stat: st}
}

func checkVersion(expected, actual int32) bool {
	return expected == zk.AnyVersion || expected == actual
}

// prep validates a request against the future state and returns the
// corresponding transaction (an error transaction if it fails).
func (ls *leaderState) prep(f *Forward, zxid int64) *Txn {
	r := f.Req
	txn := &Txn{
		Zxid: zxid, Time: time.Now().UnixMilli(),
		ServerID: f.ServerID, SessionID: f.SessionID, Xid: r.Xid,
	}
	fail := func(c zk.Code) *Txn {
		txn.Type = TxnError
		txn.Err = c
		return txn
	}

	switch r.Op {
	case zk.OpCreateSession:
		d := time.Duration(f.Timeout) * time.Millisecond
		ls.tracker[f.SessionID] = &trackedSession{timeout: d, expires: time.Now().Add(d)}
		txn.Type = TxnCreateSession
		txn.Timeout = f.Timeout
		return txn
	case zk.OpCloseSession:
		if ts := ls.tracker[f.SessionID]; ts != nil {
			ts.closing = true
		}
		return ls.closeSessionTxn(txn)
	}

	if ts := ls.tracker[f.SessionID]; ts == nil || ts.closing {
		return fail(zk.CodeSessionExpired)
	}
	if len(r.Data) > zk.MaxDataSize {
		return fail(zk.CodeBadArguments)
	}

	switch r.Op {
	case zk.OpCreate:
		path := r.Path
		parentPath := zk.Parent(path)
		pr := ls.record(parentPath)
		if r.Flags&zk.FlagSequential != 0 {
			// The counter is the parent's cversion, which only grows, so the
			// sequence of a new znode is never smaller than any earlier one.
			path = fmt.Sprintf("%s%0*d", path, zk.SequenceDigits, pr.stat.Cversion)
		}
		if zk.ValidatePath(path) != nil || path == "/" {
			return fail(zk.CodeBadArguments)
		}
		if !pr.exists {
			return fail(zk.CodeNoNode)
		}
		if pr.stat.EphemeralOwner != 0 {
			return fail(zk.CodeNoChildrenForEphemerals)
		}
		if ls.record(path).exists {
			return fail(zk.CodeNodeExists)
		}
		var owner int64
		if r.Flags&zk.FlagEphemeral != 0 {
			owner = f.SessionID
		}
		cv := pr.stat.Cversion + 1
		txn.Type, txn.Path, txn.Data, txn.Ephemeral, txn.ParentCversion = TxnCreate, path, r.Data, owner, cv

		pr.zxid, pr.stat.Cversion, pr.stat.Pzxid = zxid, cv, zxid
		pr.stat.NumChildren++
		ls.records[parentPath] = pr
		ls.records[path] = &changeRecord{zxid: zxid, exists: true,
			stat: zk.Stat{Czxid: zxid, Mzxid: zxid, Pzxid: zxid, EphemeralOwner: owner}}

	case zk.OpDelete:
		if zk.ValidatePath(r.Path) != nil || r.Path == "/" {
			return fail(zk.CodeBadArguments)
		}
		nr := ls.record(r.Path)
		if !nr.exists {
			return fail(zk.CodeNoNode)
		}
		if !checkVersion(r.Version, nr.stat.Version) {
			return fail(zk.CodeBadVersion)
		}
		if nr.stat.NumChildren > 0 {
			return fail(zk.CodeNotEmpty)
		}
		ls.deleteRecord(r.Path, zxid, func(cv int32) {
			txn.Type, txn.Path, txn.ParentCversion = TxnDelete, r.Path, cv
		})

	case zk.OpSetData:
		if zk.ValidatePath(r.Path) != nil {
			return fail(zk.CodeBadArguments)
		}
		nr := ls.record(r.Path)
		if !nr.exists {
			return fail(zk.CodeNoNode)
		}
		if !checkVersion(r.Version, nr.stat.Version) {
			return fail(zk.CodeBadVersion)
		}
		v := nr.stat.Version + 1
		txn.Type, txn.Path, txn.Data, txn.Version = TxnSetData, r.Path, r.Data, v
		nr.zxid, nr.stat.Version, nr.stat.Mzxid = zxid, v, zxid
		ls.records[r.Path] = nr

	default:
		return fail(zk.CodeBadArguments)
	}
	return txn
}

// deleteRecord updates the future state for the removal of path.
func (ls *leaderState) deleteRecord(path string, zxid int64, emit func(parentCversion int32)) {
	parentPath := zk.Parent(path)
	pr := ls.record(parentPath)
	cv := pr.stat.Cversion + 1
	emit(cv)
	pr.zxid, pr.stat.Cversion, pr.stat.Pzxid = zxid, cv, zxid
	pr.stat.NumChildren--
	ls.records[parentPath] = pr
	ls.records[path] = &changeRecord{zxid: zxid, exists: false}
}

// closeSessionTxn builds the transaction ending a session: it removes every
// ephemeral znode the session owns, including ones still being created.
func (ls *leaderState) closeSessionTxn(txn *Txn) *Txn {
	id := txn.SessionID
	ls.s.dbMu.RLock()
	candidates := ls.s.tree.EphemeralsOf(id)
	ls.s.dbMu.RUnlock()
	for p, r := range ls.records {
		if r.exists && r.stat.EphemeralOwner == id {
			candidates = append(candidates, p)
		}
	}
	sort.Strings(candidates)
	txn.Type = TxnCloseSession
	seen := map[string]bool{}
	for _, p := range candidates {
		if seen[p] {
			continue
		}
		seen[p] = true
		if r := ls.record(p); !r.exists || r.stat.EphemeralOwner != id {
			continue
		}
		ls.deleteRecord(p, txn.Zxid, func(cv int32) {
			txn.Deletes = append(txn.Deletes, DeleteOp{Path: p, ParentCversion: cv})
		})
	}
	return txn
}

// affectedPaths lists the znodes whose future state a transaction changes.
func affectedPaths(txn *Txn) []string {
	switch txn.Type {
	case TxnCreate, TxnDelete, TxnSetData:
		return []string{txn.Path, zk.Parent(txn.Path)}
	case TxnCloseSession:
		var out []string
		for _, d := range txn.Deletes {
			out = append(out, d.Path, zk.Parent(d.Path))
		}
		return out
	}
	return nil
}
