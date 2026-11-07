package web

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func call(t *testing.T, srv *httptest.Server, path, body string) map[string]any {
	t.Helper()
	var resp *http.Response
	var err error
	if body == "" {
		resp, err = http.Get(srv.URL + path)
	} else {
		resp, err = http.Post(srv.URL+path, "application/json", strings.NewReader(body))
	}
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func mustOK(t *testing.T, r map[string]any) {
	t.Helper()
	if r["ok"] != true {
		t.Fatalf("request failed: %v", r)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLabBuildClusterNodeByNode(t *testing.T) {
	lab, err := New(Options{ServerLevel: slog.LevelInfo, AppLevel: slog.LevelInfo, Tick: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer lab.Close()
	srv := httptest.NewServer(lab.Handler())
	defer srv.Close()

	// Empty at start: operations are refused with a clear message.
	if st := lab.state(); st.Size != 0 {
		t.Fatalf("expected an empty cluster, got %d nodes", st.Size)
	}
	if r := call(t, srv, "/api/znode", `{"op":"create","path":"/x"}`); r["ok"] == true {
		t.Fatal("operation should fail without nodes")
	}

	// One node serves on its own.
	mustOK(t, call(t, srv, "/api/nodes", `{"action":"add"}`))
	waitFor(t, "single node leads", func() bool { return lab.state().Leader == 1 })
	mustOK(t, call(t, srv, "/api/znode", `{"op":"create","path":"/lab","data":"x"}`))

	// Grow to 3 nodes: rolling restart, data survives, everyone converges.
	mustOK(t, call(t, srv, "/api/nodes", `{"action":"add","count":2}`))
	waitFor(t, "3 nodes with a leader", func() bool {
		st := lab.state()
		return st.Size == 3 && st.Running == 3 && st.Leader != 0
	})
	for id := 1; id <= 3; id++ {
		waitFor(t, fmt.Sprintf("node %d has /lab", id), func() bool {
			return strings.Contains(fmt.Sprint(call(t, srv, fmt.Sprintf("/api/tree?node=%d", id), "")), "/lab")
		})
	}

	// Crash the leader: the remaining two elect a new one.
	old := lab.state().Leader
	mustOK(t, call(t, srv, "/api/nodes", `{"action":"crash-leader"}`))
	waitFor(t, "new leader", func() bool { l := lab.state().Leader; return l != 0 && l != old })
	mustOK(t, call(t, srv, "/api/spawn", `{"kind":"service"}`))
	mustOK(t, call(t, srv, "/api/process", `{"name":"api-1","action":"crash"}`))

	// Restart and remove nodes.
	mustOK(t, call(t, srv, "/api/nodes", fmt.Sprintf(`{"action":"start","id":%d}`, old)))
	mustOK(t, call(t, srv, "/api/nodes", `{"action":"remove","id":3}`))
	waitFor(t, "2 nodes with a leader", func() bool { st := lab.state(); return st.Size == 2 && st.Leader != 0 })
	mustOK(t, call(t, srv, "/api/znode", `{"op":"get","path":"/lab"}`))

	mustOK(t, call(t, srv, "/api/nodes", `{"action":"reset"}`))
	if st := lab.state(); st.Size != 0 {
		t.Fatalf("reset left %d nodes", st.Size)
	}
}
