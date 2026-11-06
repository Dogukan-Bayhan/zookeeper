package tests

import (
	"fmt"
	"log"
	"os"
	"sort"
	"testing"
	"time"

	"zookeeper/client"
	"zookeeper/server"
	"zookeeper/transport"
)

const tick = 20 * time.Millisecond

// cluster is an in-process ensemble on a simulated network. Storages survive
// server restarts, simulating crashes with intact disks.
type cluster struct {
	t         testing.TB
	tr        transport.Transport
	peers     map[int64]string
	servers   map[int64]*server.Server
	storages  map[int64]server.Storage
	snapCount int
}

func newCluster(t testing.TB, n int, opts ...func(*cluster)) *cluster {
	c := &cluster{
		t:        t,
		tr:       transport.NewMemNetwork(),
		peers:    map[int64]string{},
		servers:  map[int64]*server.Server{},
		storages: map[int64]server.Storage{},
	}
	for i := 1; i <= n; i++ {
		c.peers[int64(i)] = fmt.Sprintf("zk%d", i)
		c.storages[int64(i)] = server.NewMemStorage()
	}
	for _, o := range opts {
		o(c)
	}
	for id := range c.peers {
		c.start(id)
	}
	t.Cleanup(c.shutdown)
	c.waitReady()
	return c
}

func (c *cluster) config(id int64) server.Config {
	cfg := server.Config{
		ID:        id,
		Peers:     c.peers,
		Transport: c.tr,
		Storage:   c.storages[id],
		TickTime:  tick,
		InitLimit: 25,
		SyncLimit: 8,
		SnapCount: c.snapCount,
	}
	if os.Getenv("ZK_DEBUG") != "" {
		cfg.Logger = log.New(os.Stderr, "", log.Lmicroseconds)
	}
	return cfg
}

func (c *cluster) start(id int64) {
	c.t.Helper()
	s, err := server.New(c.config(id))
	if err != nil {
		c.t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.servers[id] = s
}

func (c *cluster) stop(id int64) {
	if s := c.servers[id]; s != nil {
		s.Close()
		delete(c.servers, id)
	}
}

func (c *cluster) shutdown() {
	for id := range c.servers {
		c.stop(id)
	}
}

func (c *cluster) ids() []int64 {
	var ids []int64
	for id := range c.servers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// waitReady waits until every running server serves and agrees on a leader.
func (c *cluster) waitReady() {
	c.t.Helper()
	waitFor(c.t, 10*time.Second, "ensemble ready", func() bool {
		var leader int64
		for _, s := range c.servers {
			if !s.Serving() {
				return false
			}
			l := s.LeaderID()
			if leader != 0 && l != leader {
				return false
			}
			leader = l
		}
		return leader != 0
	})
}

func (c *cluster) leader() int64 {
	for id, s := range c.servers {
		if s.State() == server.Leading && s.Serving() {
			return id
		}
	}
	return 0
}

func (c *cluster) follower() int64 {
	for _, id := range c.ids() {
		if c.servers[id].State() == server.Following {
			return id
		}
	}
	return 0
}

// waitConverged waits until all running replicas have the same content.
func (c *cluster) waitConverged() {
	c.t.Helper()
	waitFor(c.t, 10*time.Second, "replicas converge", func() bool {
		var d [32]byte
		first := true
		for _, s := range c.servers {
			if first {
				d, first = s.Digest(), false
			} else if s.Digest() != d {
				return false
			}
		}
		return true
	})
}

func (c *cluster) addrs(ids ...int64) []string {
	if len(ids) == 0 {
		ids = c.ids()
	}
	var out []string
	for _, id := range ids {
		out = append(out, c.peers[id])
	}
	return out
}

func (c *cluster) client(ids ...int64) *client.Client {
	return c.clientCfg(client.Config{}, ids...)
}

func (c *cluster) clientCfg(cfg client.Config, ids ...int64) *client.Client {
	c.t.Helper()
	cfg.Servers = c.addrs(ids...)
	cfg.Transport = c.tr
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 3 * time.Second
	}
	cl, err := client.Connect(cfg)
	if err != nil {
		c.t.Fatalf("connect: %v", err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

func waitFor(t testing.TB, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
