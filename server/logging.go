package server

import (
	"context"
	"fmt"
	"log/slog"

	"zookeeper/zlog"
)

// Log levels used by the server:
//
//	INFO   lifecycle: elections, leader changes, synchronization, sessions,
//	       snapshots (lines starting with ★ mark major transitions), every
//	       znode change applied to the local replica, rejected writes
//	DEBUG  every request, proposal, ack, commit and watch
//	TRACE  heartbeats and raw election traffic

func (s *Server) trace(msg string, args ...any) {
	s.lg.Log(context.Background(), zlog.LevelTrace, msg, args...)
}

// logApply records a transaction applied to the local replica. Data changes
// are logged at INFO on every node, so the replication of each write is
// visible; session bookkeeping and rejected writes stay at DEBUG.
func (s *Server) logApply(t *Txn) {
	var what string
	switch t.Type {
	case TxnCreate:
		what = "✎ znode created in local replica"
	case TxnSetData:
		what = "✎ znode data changed in local replica"
	case TxnDelete:
		what = "✎ znode deleted from local replica"
	case TxnCloseSession:
		if len(t.Deletes) > 0 {
			paths := make([]string, len(t.Deletes))
			for i, d := range t.Deletes {
				paths[i] = d.Path
			}
			s.lg.Info("✎ session closed: its ephemeral znodes deleted from local replica",
				zlog.Zxid(t.Zxid), zlog.Session(t.SessionID), "paths", fmt.Sprint(paths))
			return
		}
	}
	if what == "" {
		s.lg.Debug("apply txn", txnAttrs(t)...)
		return
	}
	attrs := []any{"path", t.Path, zlog.Zxid(t.Zxid)}
	switch t.Type {
	case TxnCreate, TxnSetData:
		data := string(t.Data)
		if len(data) > 60 {
			data = data[:57] + "..."
		}
		attrs = append(attrs, "data", data)
		if t.Type == TxnSetData {
			attrs = append(attrs, "version", t.Version)
		}
		if t.Ephemeral != 0 {
			attrs = append(attrs, "ephemeral", true)
		}
	}
	s.lg.Info(what, attrs...)
}

func txnAttrs(t *Txn) []any {
	attrs := []any{zlog.Zxid(t.Zxid), "type", t.Type.String()}
	if t.Path != "" {
		attrs = append(attrs, "path", t.Path)
	}
	if t.SessionID != 0 {
		attrs = append(attrs, zlog.Session(t.SessionID))
	}
	switch t.Type {
	case TxnSetData:
		attrs = append(attrs, "newVersion", t.Version, "bytes", len(t.Data))
	case TxnCreate:
		attrs = append(attrs, "bytes", len(t.Data))
		if t.Ephemeral != 0 {
			attrs = append(attrs, "ephemeral", true)
		}
	case TxnError:
		attrs = append(attrs, "err", t.Err.Err())
	case TxnCloseSession:
		attrs = append(attrs, "ephemeralsDeleted", len(t.Deletes))
	}
	return attrs
}

func voteAttr(key string, v vote) slog.Attr {
	return slog.String(key, fmt.Sprintf("{leader:%d zxid:%s epoch:%d}", v.leader, zlog.ZxidString(v.zxid), v.epoch))
}
