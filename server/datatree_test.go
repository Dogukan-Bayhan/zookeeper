package server

import (
	"os"
	"testing"

	"zookeeper/zk"
)

func create(z int64, path string, cv int32) *Txn {
	return &Txn{Zxid: z, Type: TxnCreate, Path: path, ParentCversion: cv}
}

func set(z int64, path, data string, v int32) *Txn {
	return &Txn{Zxid: z, Type: TxnSetData, Path: path, Data: []byte(data), Version: v}
}

// TestFuzzySnapshotExample replays the example of Section 4.3: a fuzzy
// snapshot records /foo=f3 (v3) and /goo=g1 (v1), a state that never
// existed. Re-applying the idempotent transactions delivered since the
// snapshot started yields the correct state.
func TestFuzzySnapshotExample(t *testing.T) {
	tr := NewDataTree()
	tr.Apply(create(1, "/foo", 1))
	tr.Apply(create(2, "/goo", 2))
	tr.Apply(set(3, "/foo", "f1", 1))
	tr.Apply(set(4, "/goo", "g1", 1))

	stream := []*Txn{
		set(5, "/foo", "f2", 2),
		set(6, "/goo", "g2", 2),
		set(7, "/foo", "f3", 3),
	}
	want := TreeFromSnapshot(tr.ConsistentSnapshot())
	for _, txn := range stream {
		want.Apply(txn)
	}

	// The fuzzy snapshot: started at zxid 4, but /foo was read after all
	// three changes and /goo before any of them.
	fuzzy := &Snapshot{Zxid: 4, Sessions: map[int64]int64{}, Nodes: []SnapNode{
		{Path: "/", Stat: zk.Stat{Cversion: 2, Pzxid: 2}},
		{Path: "/foo", Data: []byte("f3"), Stat: zk.Stat{Version: 3, Mzxid: 7}},
		{Path: "/goo", Data: []byte("g1"), Stat: zk.Stat{Version: 1, Mzxid: 4}},
	}}
	recovered := TreeFromSnapshot(fuzzy)
	for _, txn := range stream { // Zab redelivers everything after zxid 4
		recovered.Apply(txn)
	}
	for _, p := range []string{"/foo", "/goo"} {
		d1, s1, _ := recovered.Get(p)
		d2, s2, _ := want.Get(p)
		if string(d1) != string(d2) || s1.Version != s2.Version {
			t.Fatalf("%s: recovered %q v%d, want %q v%d", p, d1, s1.Version, d2, s2.Version)
		}
	}
	if d, st, _ := recovered.Get("/foo"); string(d) != "f3" || st.Version != 3 {
		t.Fatalf("/foo = %q v%d", d, st.Version)
	}
	if d, st, _ := recovered.Get("/goo"); string(d) != "g2" || st.Version != 2 {
		t.Fatalf("/goo = %q v%d", d, st.Version)
	}
}

// TestIdempotentReplay applies a history twice in order (as after recovery
// from a fuzzy snapshot) and checks the result is unchanged.
func TestIdempotentReplay(t *testing.T) {
	history := []*Txn{
		create(1, "/a", 1),
		create(2, "/a/b", 1),
		{Zxid: 3, Type: TxnCreateSession, SessionID: 42, Timeout: 1000},
		{Zxid: 4, Type: TxnCreate, Path: "/a/e", Ephemeral: 42, ParentCversion: 2},
		set(5, "/a/b", "x", 1),
		{Zxid: 6, Type: TxnDelete, Path: "/a/b", ParentCversion: 3},
		{Zxid: 7, Type: TxnCloseSession, SessionID: 42, Deletes: []DeleteOp{{Path: "/a/e", ParentCversion: 4}}},
		create(8, "/a/c", 5),
	}
	once := NewDataTree()
	for _, txn := range history {
		once.Apply(txn)
	}
	twice := NewDataTree()
	for _, txn := range history {
		twice.Apply(txn)
	}
	for _, txn := range history[2:] {
		twice.Apply(txn)
	}
	if once.Digest() != twice.Digest() {
		t.Fatal("replay is not idempotent")
	}
	kids, st, _ := once.Children("/a")
	if len(kids) != 1 || kids[0] != "c" || st.Cversion != 5 {
		t.Fatalf("children %v cversion %d", kids, st.Cversion)
	}
	if once.HasSession(42) || len(once.EphemeralsOf(42)) != 0 {
		t.Fatal("session state not cleaned up")
	}
}

func TestFileStorageTornTail(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.Append([]*Txn{create(1, "/a", 1), create(2, "/b", 2)}); err != nil {
		t.Fatal(err)
	}
	fs.Close()
	// Simulate a crash in the middle of a write.
	f, _ := openAppend(fs.logPath())
	f.Write([]byte{0, 0, 0, 50, 1, 2, 3})
	f.Close()

	fs, err = NewFileStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	entries, _ := fs.Entries()
	if len(entries) != 2 || entries[1].Path != "/b" {
		t.Fatalf("entries after torn write: %d", len(entries))
	}
	if err := fs.Append([]*Txn{create(3, "/c", 3)}); err != nil {
		t.Fatal(err)
	}
	if entries, _ = fs.Entries(); len(entries) != 3 {
		t.Fatalf("append after recovery: %d entries", len(entries))
	}
}

func openAppend(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
}
