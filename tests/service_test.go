package tests

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"zookeeper/client"
	"zookeeper/server"
	"zookeeper/transport"
	"zookeeper/zk"
)

func TestBasicOperations(t *testing.T) {
	c := newCluster(t, 1)
	cl := c.client()

	p, err := cl.Create("/app1", []byte("config"), zk.FlagPersistent)
	must(t, err)
	if p != "/app1" {
		t.Fatalf("create returned %q", p)
	}
	if _, err := cl.Create("/app1", nil, zk.FlagPersistent); !errors.Is(err, zk.ErrNodeExists) {
		t.Fatalf("expected ErrNodeExists, got %v", err)
	}
	if _, err := cl.Create("/missing/child", nil, zk.FlagPersistent); !errors.Is(err, zk.ErrNoNode) {
		t.Fatalf("expected ErrNoNode, got %v", err)
	}
	for _, bad := range []string{"", "rel", "/a/", "/a//b", "/a/./b"} {
		if _, err := cl.Create(bad, nil, zk.FlagPersistent); !errors.Is(err, zk.ErrBadArguments) {
			t.Fatalf("path %q: expected ErrBadArguments, got %v", bad, err)
		}
	}

	data, st, err := cl.GetData("/app1", false)
	must(t, err)
	if string(data) != "config" || st.Version != 0 || st.DataLength != 6 {
		t.Fatalf("unexpected data %q stat %+v", data, st)
	}

	for i := 1; i <= 3; i++ {
		_, err := cl.Create(fmt.Sprintf("/app1/p_%d", i), nil, zk.FlagPersistent)
		must(t, err)
	}
	kids, err := cl.GetChildren("/app1", false)
	must(t, err)
	if fmt.Sprint(kids) != "[p_1 p_2 p_3]" {
		t.Fatalf("children: %v", kids)
	}
	if err := cl.Delete("/app1", zk.AnyVersion); !errors.Is(err, zk.ErrNotEmpty) {
		t.Fatalf("expected ErrNotEmpty, got %v", err)
	}

	st, err = cl.Exists("/app1/p_1", false)
	must(t, err)
	if st == nil {
		t.Fatal("exists returned nil for existing node")
	}
	must(t, cl.Delete("/app1/p_1", zk.AnyVersion))
	st, err = cl.Exists("/app1/p_1", false)
	must(t, err)
	if st != nil {
		t.Fatal("deleted node still exists")
	}
	if err := cl.Delete("/app1/p_1", zk.AnyVersion); !errors.Is(err, zk.ErrNoNode) {
		t.Fatalf("expected ErrNoNode, got %v", err)
	}
	must(t, cl.Sync("/"))
}

func TestConditionalUpdates(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	_, err := cl.Create("/cfg", []byte("v0"), zk.FlagPersistent)
	must(t, err)

	st, err := cl.SetData("/cfg", []byte("v1"), 0)
	must(t, err)
	if st.Version != 1 {
		t.Fatalf("version %d", st.Version)
	}
	if _, err := cl.SetData("/cfg", []byte("stale"), 0); !errors.Is(err, zk.ErrBadVersion) {
		t.Fatalf("expected ErrBadVersion, got %v", err)
	}
	if _, err := cl.SetData("/cfg", []byte("any"), zk.AnyVersion); err != nil {
		t.Fatal(err)
	}
	if err := cl.Delete("/cfg", 1); !errors.Is(err, zk.ErrBadVersion) {
		t.Fatalf("expected ErrBadVersion, got %v", err)
	}
	must(t, cl.Delete("/cfg", 2))

	// Many clients race on a counter with conditional updates: every
	// increment must succeed exactly once.
	_, err = cl.Create("/counter", []byte("0"), zk.FlagPersistent)
	must(t, err)
	var wg sync.WaitGroup
	const workers, incs = 4, 15
	for w := 0; w < workers; w++ {
		wc := c.client(c.ids()[w%3])
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < incs; {
				must(t, wc.Sync("/counter"))
				data, st, err := wc.GetData("/counter", false)
				must(t, err)
				var n int
				fmt.Sscan(string(data), &n)
				_, err = wc.SetData("/counter", []byte(fmt.Sprint(n+1)), st.Version)
				if errors.Is(err, zk.ErrBadVersion) {
					continue
				}
				must(t, err)
				i++
			}
		}()
	}
	wg.Wait()
	// cl may be connected to a follower that has not applied the last
	// commits yet: sync first, reads are local (Section 2.3).
	must(t, cl.Sync("/counter"))
	data, _, err := cl.GetData("/counter", false)
	must(t, err)
	if string(data) != fmt.Sprint(workers*incs) {
		t.Fatalf("counter = %s, want %d", data, workers*incs)
	}
}

func TestReplication(t *testing.T) {
	c := newCluster(t, 5)
	var clients []*client.Client
	for _, id := range c.ids() {
		clients = append(clients, c.client(id))
	}
	must(t, errOnly(clients[0].Create("/r", nil, zk.FlagPersistent)))
	var wg sync.WaitGroup
	for i, cl := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, err := cl.Create(fmt.Sprintf("/r/c%d-%d", i, j), []byte{byte(j)}, zk.FlagPersistent)
				must(t, err)
			}
		}()
	}
	wg.Wait()
	c.waitConverged()
	for _, cl := range clients {
		must(t, cl.Sync("/r"))
		kids, err := cl.GetChildren("/r", false)
		must(t, err)
		if len(kids) != 100 {
			t.Fatalf("got %d children", len(kids))
		}
	}
}

func errOnly(_ any, err error) error { return err }

func TestSequentialAndEphemeral(t *testing.T) {
	c := newCluster(t, 3)
	a := c.client()
	b := c.client()
	must(t, errOnly(a.Create("/q", nil, zk.FlagPersistent)))

	var names []string
	for i := 0; i < 5; i++ {
		cl := a
		if i%2 == 1 {
			cl = b
		}
		n, err := cl.Create("/q/item-", nil, zk.FlagSequential)
		must(t, err)
		names = append(names, n)
	}
	for i := 1; i < len(names); i++ {
		p, _ := zk.SequenceOf(names[i-1])
		n, _ := zk.SequenceOf(names[i])
		if n <= p {
			t.Fatalf("sequence not increasing: %v", names)
		}
	}
	if names[0] != "/q/item-0000000000" {
		t.Fatalf("first name %q", names[0])
	}

	// The sequence never goes backwards, even after deletions.
	must(t, a.Delete(names[4], zk.AnyVersion))
	n, err := a.Create("/q/item-", nil, zk.FlagSequential)
	must(t, err)
	last, _ := zk.SequenceOf(names[4])
	if s, _ := zk.SequenceOf(n); s <= last {
		t.Fatalf("sequence reused: %s after %s", n, names[4])
	}

	// Ephemeral znodes go away with their session and cannot have children.
	eph, err := b.Create("/q/eph", []byte("x"), zk.FlagEphemeral)
	must(t, err)
	if _, err := b.Create("/q/eph/child", nil, zk.FlagPersistent); !errors.Is(err, zk.ErrNoChildrenForEphemerals) {
		t.Fatalf("expected ErrNoChildrenForEphemerals, got %v", err)
	}
	must(t, a.Sync(eph))
	st, err := a.Exists(eph, false)
	must(t, err)
	if st == nil || st.EphemeralOwner != b.SessionID() {
		t.Fatalf("bad ephemeral stat %+v", st)
	}
	_, ch, err := a.ExistsW(eph)
	must(t, err)
	b.Close()
	expectEvent(t, ch, zk.EventNodeDeleted, eph)
	if st, _ := a.Exists(eph, false); st != nil {
		t.Fatal("ephemeral survived session close")
	}
	c.waitConverged()
}

func expectEvent(t *testing.T, ch <-chan zk.Event, typ zk.EventType, path string) {
	t.Helper()
	select {
	case ev := <-ch:
		if ev.Type != typ || ev.Path != path {
			t.Fatalf("got %v, want %v %s", ev, typ, path)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no %v event for %s", typ, path)
	}
}

func expectNoEvent(t *testing.T, ch <-chan zk.Event) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("unexpected event %v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWatches(t *testing.T) {
	c := newCluster(t, 3)
	ids := c.ids()
	watcher := c.client(ids[0])
	writer := c.client(ids[1])

	// exists watch on a missing node fires on creation.
	st, ch, err := watcher.ExistsW("/w")
	must(t, err)
	if st != nil {
		t.Fatal("unexpected node")
	}
	must(t, errOnly(writer.Create("/w", []byte("1"), zk.FlagPersistent)))
	expectEvent(t, ch, zk.EventNodeCreated, "/w")

	// Data watches are one-time triggers: two changes, one event.
	_, _, ch, err = watcher.GetDataW("/w")
	must(t, err)
	must(t, errOnly(writer.SetData("/w", []byte("2"), zk.AnyVersion)))
	must(t, errOnly(writer.SetData("/w", []byte("3"), zk.AnyVersion)))
	expectEvent(t, ch, zk.EventNodeDataChanged, "/w")
	expectNoEvent(t, ch)

	// Child watches.
	_, ch, err = watcher.GetChildrenW("/w")
	must(t, err)
	must(t, errOnly(writer.Create("/w/k", nil, zk.FlagPersistent)))
	expectEvent(t, ch, zk.EventNodeChildrenChanged, "/w")

	// Delete fires data and child watches.
	_, _, dch, err := watcher.GetDataW("/w/k")
	must(t, err)
	_, cch, err := watcher.GetChildrenW("/w/k")
	must(t, err)
	must(t, writer.Delete("/w/k", zk.AnyVersion))
	expectEvent(t, dch, zk.EventNodeDeleted, "/w/k")
	expectEvent(t, cch, zk.EventNodeDeleted, "/w/k")

	// The notification arrives before the client can observe the new state:
	// once a read reflects the change, the event must already be queued.
	var mu sync.Mutex
	var log []string
	def := c.clientCfg(client.Config{Watcher: func(ev zk.Event) {
		if ev.Type != zk.EventSession {
			mu.Lock()
			log = append(log, "event:"+ev.Type.String())
			mu.Unlock()
		}
	}}, ids[2])
	must(t, def.Sync("/w"))
	_, _, err = def.GetData("/w", true)
	must(t, err)
	must(t, errOnly(writer.SetData("/w", []byte("new"), zk.AnyVersion)))
	done := make(chan struct{})
	var poll func()
	poll = func() {
		def.GetDataAsync("/w", false, func(data []byte, _ *zk.Stat, err error) {
			mu.Lock()
			if err == nil && string(data) == "new" {
				log = append(log, "read:new")
				mu.Unlock()
				close(done)
				return
			}
			mu.Unlock()
			poll()
		})
	}
	poll()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(log) < 2 || log[0] != "event:NodeDataChanged" {
		t.Fatalf("event must precede new state, got %v", log)
	}
}

// TestSessionExpiry uses a raw connection that creates a session and an
// ephemeral znode, then goes silent: the leader must expire the session.
func TestSessionExpiry(t *testing.T) {
	c := newCluster(t, 3)
	observer := c.client()
	conn, err := c.tr.Dial(c.peers[c.follower()], time.Second)
	must(t, err)
	must(t, conn.Send(&zk.ConnectRequest{Timeout: 300}))
	m, err := conn.Recv()
	must(t, err)
	cr := m.(*zk.ConnectResponse)
	if cr.Err != zk.CodeOK {
		t.Fatalf("connect: %v", cr.Err)
	}
	must(t, conn.Send(&zk.Request{Xid: 1, Op: zk.OpCreate, Path: "/lease", Flags: zk.FlagEphemeral}))
	m, err = conn.Recv()
	must(t, err)
	if r := m.(*zk.Response); r.Err != zk.CodeOK {
		t.Fatalf("create: %v", r.Err)
	}
	must(t, observer.Sync("/lease"))
	_, ch, err := observer.ExistsW("/lease")
	must(t, err)
	start := time.Now()
	expectEvent(t, ch, zk.EventNodeDeleted, "/lease")
	if time.Since(start) < 150*time.Millisecond {
		t.Fatalf("session expired too early (%v)", time.Since(start))
	}
	conn.Close()

	// Reconnecting with the expired session is refused.
	conn2, err := c.tr.Dial(c.peers[c.leader()], time.Second)
	must(t, err)
	defer conn2.Close()
	must(t, conn2.Send(&zk.ConnectRequest{SessionID: cr.SessionID, Timeout: 300}))
	m, err = conn2.Recv()
	must(t, err)
	if m.(*zk.ConnectResponse).Err != zk.CodeSessionExpired {
		t.Fatalf("expected expired session, got %v", m.(*zk.ConnectResponse).Err)
	}
}

// TestAsyncPipelineFIFO issues many asynchronous operations at once; the
// callbacks must run in the order the operations were issued.
func TestAsyncPipelineFIFO(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client(c.follower())
	must(t, errOnly(cl.Create("/cfg", nil, zk.FlagPersistent)))

	const n = 500
	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup
	wg.Add(2*n + 1)
	record := func(i int) {
		mu.Lock()
		order = append(order, i)
		mu.Unlock()
		wg.Done()
	}
	// The "ready" pattern of Section 2.3: delete ready, update the config
	// znodes asynchronously, then re-create ready.
	cl.DeleteAsync("/cfg/ready", zk.AnyVersion, func(error) { record(-1) })
	for i := 0; i < n; i++ {
		cl.CreateAsync(fmt.Sprintf("/cfg/p%04d", i), []byte{byte(i)}, zk.FlagPersistent, func(_ string, err error) {
			if err != nil {
				t.Error(err)
			}
			record(2 * i)
		})
		// A read issued after the write must observe it (FIFO client order).
		cl.ExistsAsync(fmt.Sprintf("/cfg/p%04d", i), false, func(st *zk.Stat, err error) {
			if err != nil || st == nil {
				t.Errorf("read after write did not see p%04d: %v", i, err)
			}
			record(2*i + 1)
		})
	}
	wg.Wait()
	must(t, errOnly(cl.Create("/cfg/ready", nil, zk.FlagPersistent)))
	for i := 1; i < len(order); i++ {
		if order[i] != order[i-1]+1 {
			t.Fatalf("callbacks out of order at %d: %v", i, order[max(0, i-3):i+1])
		}
	}
}

// TestSyncSlowRead: B learns about A's write through another channel; with
// sync followed by a read, B is guaranteed to observe it.
func TestSyncSlowRead(t *testing.T) {
	c := newCluster(t, 5)
	ids := c.ids()
	a := c.client(ids[0])
	b := c.client(ids[4])
	must(t, errOnly(a.Create("/shared", []byte("0"), zk.FlagPersistent)))
	for i := 1; i <= 50; i++ {
		v := []byte(fmt.Sprint(i))
		must(t, errOnly(a.SetData("/shared", v, zk.AnyVersion)))
		must(t, b.Sync("/shared"))
		data, _, err := b.GetData("/shared", false)
		must(t, err)
		if !bytes.Equal(data, v) {
			t.Fatalf("slow read returned %s, want %s", data, v)
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 5)
	cl := c.client()
	must(t, errOnly(cl.Create("/data", nil, zk.FlagPersistent)))
	for i := 0; i < 20; i++ {
		must(t, errOnly(cl.Create(fmt.Sprintf("/data/n%d", i), nil, zk.FlagPersistent)))
	}
	eph, err := cl.Create("/data/eph", nil, zk.FlagEphemeral)
	must(t, err)

	for round := 0; round < 2; round++ {
		old := c.leader()
		c.stop(old)
		c.waitReady()
		if c.leader() == old {
			t.Fatal("dead leader still leading")
		}
		// The session survives the failover (it moves to another server).
		for i := 0; ; i++ {
			_, err := cl.Create(fmt.Sprintf("/data/after%d", round), nil, zk.FlagPersistent)
			if err == nil {
				break
			}
			if !errors.Is(err, zk.ErrConnectionLoss) || i > 50 {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		kids, err := cl.GetChildren("/data", false)
		must(t, err)
		if len(kids) != 22+round {
			t.Fatalf("round %d: %d children: %v", round, len(kids), kids)
		}
		if st, _ := cl.Exists(eph, false); st == nil {
			t.Fatal("ephemeral of a live session lost on failover")
		}
		c.start(old)
		c.waitReady()
		c.waitConverged()
	}
}

func TestFollowerRecovery(t *testing.T) {
	c := newCluster(t, 3, func(c *cluster) { c.snapCount = 50 })
	cl := c.client(c.leader())
	must(t, errOnly(cl.Create("/x", nil, zk.FlagPersistent)))

	// A short outage: the follower catches up with a DIFF.
	f := c.follower()
	c.stop(f)
	for i := 0; i < 20; i++ {
		must(t, errOnly(cl.Create(fmt.Sprintf("/x/a%d", i), []byte("a"), zk.FlagPersistent)))
	}
	c.start(f)
	c.waitReady()
	c.waitConverged()

	// A long outage spanning several snapshots: the follower gets a SNAP.
	c.stop(f)
	for i := 0; i < 300; i++ {
		must(t, errOnly(cl.Create(fmt.Sprintf("/x/b%d", i), []byte("b"), zk.FlagPersistent)))
	}
	c.start(f)
	c.waitReady()
	c.waitConverged()
	if n := c.servers[f].NodeCount(); n != 322 {
		t.Fatalf("recovered follower has %d nodes", n)
	}

	// The whole ensemble restarts from its logs and snapshots.
	for _, id := range c.ids() {
		c.stop(id)
	}
	for id := range c.peers {
		c.start(id)
	}
	c.waitReady()
	c.waitConverged()
	cl2 := c.client()
	kids, err := cl2.GetChildren("/x", false)
	must(t, err)
	if len(kids) != 320 {
		t.Fatalf("after full restart: %d children", len(kids))
	}
}

func TestMinorityCannotWrite(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	must(t, errOnly(cl.Create("/q", nil, zk.FlagPersistent)))
	ids := c.ids()
	c.stop(ids[0])
	c.stop(ids[1])
	waitFor(t, 5*time.Second, "survivor stops serving", func() bool { return !c.servers[ids[2]].Serving() })

	// Bring a second server back: a quorum exists again and data survived.
	c.start(ids[1])
	c.waitReady()
	cl2 := c.client()
	st, err := cl2.Exists("/q", false)
	must(t, err)
	if st == nil {
		t.Fatal("committed data lost")
	}
}

func TestFileStorageRecovery(t *testing.T) {
	dir := t.TempDir()
	newServer := func() (*server.Server, *server.FileStorage, transport.Transport) {
		st, err := server.NewFileStorage(dir, false)
		must(t, err)
		net := transport.NewMemNetwork()
		s, err := server.New(server.Config{ID: 1, Peers: map[int64]string{1: "solo"}, Transport: net,
			Storage: st, TickTime: tick, SnapCount: 25})
		must(t, err)
		must(t, s.Start())
		waitFor(t, 5*time.Second, "serving", s.Serving)
		return s, st, net
	}
	s, st, net := newServer()
	cl, err := client.Connect(client.Config{Servers: []string{"solo"}, Transport: net, SessionTimeout: time.Second})
	must(t, err)
	must(t, errOnly(cl.Create("/persist", []byte("hello"), zk.FlagPersistent)))
	for i := 0; i < 60; i++ {
		must(t, errOnly(cl.SetData("/persist", []byte(fmt.Sprintf("v%d", i)), zk.AnyVersion)))
	}
	cl.Close()
	digest := s.Digest()
	s.Close()
	st.Close()

	s, st, net = newServer()
	defer st.Close()
	defer s.Close()
	if s.Digest() != digest {
		t.Fatal("state differs after restart")
	}
	cl, err = client.Connect(client.Config{Servers: []string{"solo"}, Transport: net, SessionTimeout: time.Second})
	must(t, err)
	defer cl.Close()
	data, stat, err := cl.GetData("/persist", false)
	must(t, err)
	if string(data) != "v59" || stat.Version != 60 {
		t.Fatalf("got %q version %d", data, stat.Version)
	}
}

func TestClientMovesWithWatches(t *testing.T) {
	c := newCluster(t, 3)
	f := c.follower()
	watcher := c.client(f, c.leader())
	writer := c.client(c.leader())
	must(t, errOnly(writer.Create("/m", nil, zk.FlagPersistent)))
	must(t, watcher.Sync("/m")) // reads are local: sync to see the other client's write
	_, _, ch, err := watcher.GetDataW("/m")
	must(t, err)
	sid := watcher.SessionID()

	// The watcher's server dies; the session and its watch move to another
	// server. The change made meanwhile is reported on re-registration.
	c.stop(f)
	must(t, errOnly(writer.SetData("/m", []byte("changed"), zk.AnyVersion)))
	expectEvent(t, ch, zk.EventNodeDataChanged, "/m")
	if watcher.SessionID() != sid {
		t.Fatal("session changed")
	}
}

func TestTCPEnsemble(t *testing.T) {
	peers := map[int64]string{}
	for i := int64(1); i <= 3; i++ {
		peers[i] = freeAddr(t)
	}
	var servers []*server.Server
	for id := range peers {
		s, err := server.New(server.Config{ID: id, Peers: peers, TickTime: 50 * time.Millisecond})
		must(t, err)
		must(t, s.Start())
		servers = append(servers, s)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()
	waitFor(t, 15*time.Second, "tcp ensemble ready", func() bool {
		for _, s := range servers {
			if !s.Serving() {
				return false
			}
		}
		return true
	})
	var addrs []string
	for _, a := range peers {
		addrs = append(addrs, a)
	}
	cl, err := client.Connect(client.Config{Servers: addrs, SessionTimeout: 5 * time.Second})
	must(t, err)
	defer cl.Close()
	must(t, errOnly(cl.Create("/tcp", []byte("over the wire"), zk.FlagPersistent)))
	_, ch, err := cl.GetChildrenW("/tcp")
	must(t, err)
	must(t, errOnly(cl.Create("/tcp/child", nil, zk.FlagEphemeral)))
	expectEvent(t, ch, zk.EventNodeChildrenChanged, "/tcp")
	data, _, err := cl.GetData("/tcp", false)
	must(t, err)
	if string(data) != "over the wire" {
		t.Fatalf("got %q", data)
	}
}
