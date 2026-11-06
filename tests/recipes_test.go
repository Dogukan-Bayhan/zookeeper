package tests

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"zookeeper/client"
	"zookeeper/recipes"
	"zookeeper/zk"
)

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	defer l.Close()
	return l.Addr().String()
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestConfigManagement(t *testing.T) {
	c := newCluster(t, 3)
	admin := c.client()
	ctx, cancel := context.WithCancel(ctxT(t))
	defer cancel()

	seen := make(chan string, 100)
	go recipes.WatchConfig(ctx, c.client(), "/zc", func(data []byte, _ *zk.Stat) { seen <- string(data) })
	time.Sleep(50 * time.Millisecond)
	must(t, errOnly(admin.Create("/zc", []byte("v1"), zk.FlagPersistent)))
	waitValue(t, seen, "v1")
	for i := 2; i <= 5; i++ {
		must(t, errOnly(admin.SetData("/zc", []byte(fmt.Sprintf("v%d", i)), zk.AnyVersion)))
	}
	// Intermediate versions may be skipped, but the latest one is always seen.
	waitValue(t, seen, "v5")
}

func waitValue(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case v := <-ch:
			if v == want {
				return
			}
		case <-timeout:
			t.Fatalf("never saw %q", want)
		}
	}
}

func TestRendezvous(t *testing.T) {
	c := newCluster(t, 3)
	master := c.client()
	r, err := recipes.NewRendezvous(master, "/zr", true)
	must(t, err)

	var wg sync.WaitGroup
	results := make(chan string, 3)
	for i := 0; i < 3; i++ {
		w := c.client()
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := recipes.OpenRendezvous(w, "/zr").Wait(ctxT(t))
			must(t, err)
			results <- string(info)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	must(t, r.Publish([]byte("master@10.0.0.1:9000")))
	wg.Wait()
	close(results)
	for v := range results {
		if v != "master@10.0.0.1:9000" {
			t.Fatalf("worker got %q", v)
		}
	}
}

func TestGroupMembership(t *testing.T) {
	c := newCluster(t, 3)
	admin := c.client()
	must(t, recipes.EnsurePath(admin, "/zg"))
	ctx, cancel := context.WithCancel(ctxT(t))
	defer cancel()
	views := make(chan []string, 100)
	go recipes.WatchMembers(ctx, admin, "/zg", func(m []string) { views <- m })

	var members []*client.Client
	for i := 0; i < 3; i++ {
		m := c.client()
		_, err := recipes.JoinGroup(m, "/zg", fmt.Sprintf("p_%d", i), []byte("addr"))
		must(t, err)
		members = append(members, m)
	}
	_, err := recipes.JoinGroup(c.client(), "/zg", "", nil)
	must(t, err)
	waitView(t, views, 4)
	// A member that ends (or fails) disappears automatically.
	members[1].Close()
	got := waitView(t, views, 3)
	for _, m := range got {
		if m == "p_1" {
			t.Fatalf("p_1 still a member: %v", got)
		}
	}
}

func waitView(t *testing.T, ch <-chan []string, n int) []string {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case v := <-ch:
			if len(v) == n {
				return v
			}
		case <-timeout:
			t.Fatalf("membership never reached %d", n)
		}
	}
}

// mutualExclusion runs workers that increment a shared variable inside the
// critical section and checks that no two are ever inside at once.
func mutualExclusion(t *testing.T, n, iters int, lock func(cl *client.Client) (func(context.Context) error, func() error), c *cluster) {
	var inside, maxInside, total atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		cl := c.client(c.ids()[i%len(c.ids())])
		acquire, release := lock(cl)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				must(t, acquire(ctxT(t)))
				v := inside.Add(1)
				if v > maxInside.Load() {
					maxInside.Store(v)
				}
				time.Sleep(time.Millisecond)
				total.Add(1)
				inside.Add(-1)
				must(t, release())
			}
		}()
	}
	wg.Wait()
	if maxInside.Load() != 1 {
		t.Fatalf("%d clients held the lock simultaneously", maxInside.Load())
	}
	if int(total.Load()) != n*iters {
		t.Fatalf("total %d", total.Load())
	}
}

func TestSimpleLock(t *testing.T) {
	c := newCluster(t, 3)
	mutualExclusion(t, 4, 5, func(cl *client.Client) (func(context.Context) error, func() error) {
		l := recipes.NewSimpleLock(cl, "/simple-lock")
		return l.Lock, l.Unlock
	}, c)
}

func TestLockWithoutHerdEffect(t *testing.T) {
	c := newCluster(t, 3)
	mutualExclusion(t, 5, 5, func(cl *client.Client) (func(context.Context) error, func() error) {
		l := recipes.NewLock(cl, "/locks/l")
		return l.Lock, l.Unlock
	}, c)
}

func TestLockReleasedOnCrash(t *testing.T) {
	c := newCluster(t, 3)
	holder := c.client()
	l1 := recipes.NewLock(holder, "/locks/crash")
	must(t, l1.Lock(ctxT(t)))

	l2 := recipes.NewLock(c.client(), "/locks/crash")
	acquired := make(chan error, 1)
	go func() { acquired <- l2.Lock(ctxT(t)) }()
	select {
	case <-acquired:
		t.Fatal("lock acquired while held")
	case <-time.After(100 * time.Millisecond):
	}
	holder.Close() // the session ends: its ephemeral lock znode goes away
	select {
	case err := <-acquired:
		must(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("lock not released when holder's session ended")
	}
}

func TestReadWriteLock(t *testing.T) {
	c := newCluster(t, 3)
	var readers, writers, maxReaders atomic.Int32
	var violations atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		cl := c.client()
		write := i%3 == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 4; j++ {
				l := recipes.NewRWLock(cl, "/rw")
				if write {
					must(t, l.Lock(ctxT(t)))
					if writers.Add(1) != 1 || readers.Load() != 0 {
						violations.Add(1)
					}
					time.Sleep(3 * time.Millisecond)
					writers.Add(-1)
				} else {
					must(t, l.RLock(ctxT(t)))
					r := readers.Add(1)
					if writers.Load() != 0 {
						violations.Add(1)
					}
					if r > maxReaders.Load() {
						maxReaders.Store(r)
					}
					time.Sleep(5 * time.Millisecond)
					readers.Add(-1)
				}
				must(t, l.Unlock())
			}
		}()
	}
	wg.Wait()
	if violations.Load() != 0 {
		t.Fatalf("%d exclusion violations", violations.Load())
	}
	if maxReaders.Load() < 2 {
		t.Logf("note: readers never overlapped (max %d)", maxReaders.Load())
	}
}

func TestDoubleBarrier(t *testing.T) {
	c := newCluster(t, 3)
	const n, rounds = 5, 3
	var arrived, finished, left atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan string, 2*n*rounds)
	for i := 0; i < n; i++ {
		cl := c.client()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				b := recipes.NewDoubleBarrier(cl, fmt.Sprintf("/barrier/b%d", round), fmt.Sprintf("p%d", i), n)
				time.Sleep(time.Duration(i) * 3 * time.Millisecond)
				arrived.Add(1)
				must(t, b.Enter(ctxT(t)))
				// Nobody passes Enter before all n processes arrived.
				if arrived.Load() < int32((round+1)*n) {
					errs <- fmt.Sprintf("round %d: entered before everybody arrived", round)
				}
				time.Sleep(time.Duration(n-i) * 2 * time.Millisecond) // the computation
				finished.Add(1)
				must(t, b.Leave(ctxT(t)))
				// Nobody passes Leave before all n processes finished.
				if finished.Load() < int32((round+1)*n) {
					errs <- fmt.Sprintf("round %d: left before everybody finished", round)
				}
				left.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	if left.Load() != n*rounds {
		t.Fatalf("left %d", left.Load())
	}
}

func TestLeaderElection(t *testing.T) {
	c := newCluster(t, 3)
	type candidate struct {
		cl *client.Client
		e  *recipes.Election
	}
	var cands []candidate
	elected := make(chan int, 3)
	for i := 0; i < 3; i++ {
		cl := c.client()
		e := recipes.NewElection(cl, "/election")
		cands = append(cands, candidate{cl, e})
		go func() {
			if err := e.Campaign(ctxT(t), []byte(fmt.Sprint(i))); err == nil {
				elected <- i
			}
		}()
	}
	first := <-elected
	data, err := recipes.Leader(c.client(), "/election")
	must(t, err)
	if string(data) != fmt.Sprint(first) {
		t.Fatalf("leader data %s, elected %d", data, first)
	}
	select {
	case other := <-elected:
		t.Fatalf("two leaders: %d and %d", first, other)
	case <-time.After(100 * time.Millisecond):
	}
	// The leader crashes; exactly one other candidate takes over.
	cands[first].cl.Close()
	second := <-elected
	if second == first {
		t.Fatal("same leader re-elected")
	}
	cands[second].e.Resign()
	third := <-elected
	got := []int{first, second, third}
	sort.Ints(got)
	if fmt.Sprint(got) != "[0 1 2]" {
		t.Fatalf("elections: %v", got)
	}
}
