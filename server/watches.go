package server

import (
	"log/slog"
	"sync"

	"zookeeper/zk"
	"zookeeper/zlog"
)

// watchManager keeps the one-time watches registered by the sessions
// connected to this server. Only the server a client is connected to tracks
// and triggers notifications for that client (Section 4.4).
type watchManager struct {
	mu    sync.Mutex
	data  map[string]map[*clientSession]struct{} // getData / exists watches
	child map[string]map[*clientSession]struct{} // getChildren watches
	lg    *slog.Logger
}

func newWatchManager() *watchManager {
	return &watchManager{
		data:  map[string]map[*clientSession]struct{}{},
		child: map[string]map[*clientSession]struct{}{},
		lg:    zlog.Discard(),
	}
}

func add(m map[string]map[*clientSession]struct{}, path string, cs *clientSession) {
	set := m[path]
	if set == nil {
		set = map[*clientSession]struct{}{}
		m[path] = set
	}
	set[cs] = struct{}{}
}

func (w *watchManager) addData(path string, cs *clientSession) {
	w.mu.Lock()
	add(w.data, path, cs)
	w.mu.Unlock()
}

func (w *watchManager) addChild(path string, cs *clientSession) {
	w.mu.Lock()
	add(w.child, path, cs)
	w.mu.Unlock()
}

// trigger fires and unregisters the watches matching a change. It is called
// while the data tree write lock is held, so the notification is queued on
// the client connection before any read can observe the new state.
func (w *watchManager) trigger(path string, typ zk.EventType, zxid int64) {
	w.mu.Lock()
	targets := map[*clientSession]struct{}{}
	take := func(m map[string]map[*clientSession]struct{}) {
		for cs := range m[path] {
			targets[cs] = struct{}{}
		}
		delete(m, path)
	}
	switch typ {
	case zk.EventNodeCreated, zk.EventNodeDataChanged:
		take(w.data)
	case zk.EventNodeDeleted:
		take(w.data)
		take(w.child)
	case zk.EventNodeChildrenChanged:
		take(w.child)
	}
	w.mu.Unlock()
	ev := &zk.WatcherEvent{Type: typ, Path: path, Zxid: zxid}
	for cs := range targets {
		w.lg.Debug("watch triggered: notifying client", "event", typ.String(), "path", path, zlog.Session(cs.id), zlog.Zxid(zxid))
		cs.send(ev)
	}
}

func (w *watchManager) removeSession(cs *clientSession) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, m := range []map[string]map[*clientSession]struct{}{w.data, w.child} {
		for p, set := range m {
			delete(set, cs)
			if len(set) == 0 {
				delete(m, p)
			}
		}
	}
}

func (w *watchManager) clear() {
	w.mu.Lock()
	w.data = map[string]map[*clientSession]struct{}{}
	w.child = map[string]map[*clientSession]struct{}{}
	w.mu.Unlock()
}
