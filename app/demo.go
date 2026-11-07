package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"zookeeper/zlog"
)

// Demo runs the scenarios against an in-process ensemble, narrating each
// step and injecting failures (crashed processes, crashed ZooKeeper leader).
type Demo struct {
	Env      *Env
	Ensemble *Ensemble
	Sink     *zlog.Sink
	Pause    time.Duration

	ctx   context.Context
	procs []*Process
	mu    sync.Mutex
	fails int
}

// Processes returns the processes started by the running scenario.
func (d *Demo) Processes() []*Process {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*Process(nil), d.procs...)
}

// Failures returns the number of failed checks.
func (d *Demo) Failures() int { return d.fails }

// Scenarios lists the available scenarios in order.
var Scenarios = []string{"config", "discovery", "election", "lock", "queue", "failover"}

// Run runs the named scenarios.
func (d *Demo) Run(ctx context.Context, names []string) error {
	d.ctx = ctx
	for i, name := range names {
		var err error
		switch name {
		case "config":
			err = d.config(i+1, len(names))
		case "discovery":
			err = d.discovery(i+1, len(names))
		case "election":
			err = d.election(i+1, len(names))
		case "lock":
			err = d.lock(i+1, len(names))
		case "queue":
			err = d.queue(i+1, len(names))
		case "failover":
			err = d.failover(i+1, len(names))
		default:
			return fmt.Errorf("unknown scenario %q (choose from %s)", name, strings.Join(Scenarios, ", "))
		}
		d.stopAll()
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
	if d.fails == 0 {
		d.Sink.Banner("ALL SCENARIOS PASSED ✔")
	} else {
		d.Sink.Banner(fmt.Sprintf("%d CHECK(S) FAILED ✘", d.fails))
	}
	return nil
}

func (d *Demo) start(name string) *Process {
	p, err := d.Env.Start(d.ctx, name)
	if err != nil {
		panic(err)
	}
	d.mu.Lock()
	d.procs = append(d.procs, p)
	d.mu.Unlock()
	return p
}

func (d *Demo) stopAll() {
	d.mu.Lock()
	procs := d.procs
	d.procs = nil
	d.mu.Unlock()
	for _, p := range procs {
		p.Stop()
	}
}

func (d *Demo) wait(mult float64) bool {
	return sleepCtx(d.ctx, time.Duration(float64(d.Pause)*mult))
}

func (d *Demo) check(ok bool, format string, args ...any) {
	if !ok {
		d.fails++
	}
	d.Sink.Result(ok, format, args...)
}

func (d *Demo) tree(root string) {
	p := d.start("inspector")
	defer p.Stop()
	d.Sink.Println("")
	d.Sink.Println("   znode tree " + root + ":")
	PrintTree(p.Client, root, func(line string) { d.Sink.Println("     " + line) })
	d.Sink.Println("")
}

func (d *Demo) waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		if !sleepCtx(d.ctx, 20*time.Millisecond) {
			return false
		}
	}
	return cond()
}

// ---------------------------------------------------------------------------

func (d *Demo) config(i, n int) error {
	d.Sink.Banner(fmt.Sprintf("SCENARIO %d/%d · Dynamic configuration (no restarts)", i, n),
		"3 instances of a shop service watch "+ConfigPath+".",
		"An operator changes rate limits and feature flags at runtime; every instance is",
		"notified through its watch, re-reads the znode and applies the new settings live.",
		"Two operators editing at the same time are serialized by conditional setData (version).")

	var services []*ConfigurableService
	for _, name := range []string{"shop-1", "shop-2", "shop-3"} {
		s := NewConfigurableService(d.start(name))
		services = append(services, s)
		go s.Run()
	}
	admin := d.start("operator")
	d.wait(1)

	d.Sink.Step("The operator publishes the initial configuration")
	SetConfig(admin.Client, Settings{"rateLimit": "100", "feature.newCheckout": "off", "logLevel": "info"})
	d.wait(1)

	d.Sink.Step("Incident! The operator lowers the rate limit and enables debug logs — instances react within milliseconds")
	SetConfig(admin.Client, Settings{"rateLimit": "20", "logLevel": "debug"})
	d.wait(1)

	d.Sink.Step("Two operators edit different keys at the same moment: optimistic concurrency with versions")
	admin2 := d.start("operator-2")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); SetConfig(admin.Client, Settings{"feature.newCheckout": "on"}) }()
	go func() { defer wg.Done(); SetConfig(admin2.Client, Settings{"rateLimit": "200"}) }()
	wg.Wait()
	d.wait(1)

	d.Sink.Step("Checking that every instance converged to the same configuration")
	ok := d.waitUntil(5*time.Second, func() bool {
		for _, s := range services {
			cur, _ := s.Current()
			if cur["feature.newCheckout"] != "on" || cur["rateLimit"] != "200" {
				return false
			}
		}
		return true
	})
	for _, s := range services {
		cur, v := s.Current()
		d.Sink.Println(fmt.Sprintf("     %s → version %d %s", s.p.Name, v, fmtSettings(cur)))
	}
	d.check(ok, "all instances applied both concurrent edits (no lost update)")
	d.tree("/apps/shop")
	return nil
}

// ---------------------------------------------------------------------------

func (d *Demo) discovery(i, n int) error {
	d.Sink.Banner(fmt.Sprintf("SCENARIO %d/%d · Service discovery + load balancing", i, n),
		"Real HTTP servers register an EPHEMERAL|SEQUENTIAL znode under /services/api with",
		"their address. A gateway watches the registry and load-balances requests over the",
		"live instances. We scale up, crash an instance and shut one down gracefully.")

	var instances []*ServiceInstance
	for _, name := range []string{"api-1", "api-2", "api-3"} {
		si, err := StartServiceInstance(d.start(name), "api")
		if err != nil {
			return err
		}
		instances = append(instances, si)
	}
	gw := NewGateway(d.start("gateway"), "api")
	go gw.Run(d.Pause / 3)
	d.wait(2)

	d.Sink.Step("Autoscaling: a 4th instance starts and registers — the gateway picks it up via its watch")
	si4, err := StartServiceInstance(d.start("api-4"), "api")
	if err != nil {
		return err
	}
	instances = append(instances, si4)
	d.wait(2)

	d.Sink.Step("api-2 CRASHES (machine dies). Requests to it fail and are retried elsewhere until its session expires")
	instances[1].p.Crash()
	ok := d.waitUntil(3*d.Env.SessionTimeout, func() bool { return len(gw.Instances()) == 3 })
	d.check(ok, "crashed instance removed from the routing table after its session timeout")
	d.wait(1)

	d.Sink.Step("api-3 shuts down gracefully: it closes its session, so it disappears immediately")
	start := time.Now()
	instances[2].p.Stop()
	ok = d.waitUntil(5*time.Second, func() bool { return len(gw.Instances()) == 2 })
	d.check(ok, "gracefully stopped instance removed in %v", time.Since(start).Round(time.Millisecond))
	d.wait(2)

	okReq, failReq := gw.Stats()
	d.check(okReq > 0, "gateway served %d requests (%d could not be routed)", okReq, failReq)
	for _, si := range instances {
		d.Sink.Println(fmt.Sprintf("     %s served %d requests", si.p.Name, si.Served()))
	}
	d.tree(ServicesRoot)
	return nil
}

// ---------------------------------------------------------------------------

func (d *Demo) election(i, n int) error {
	d.Sink.Banner(fmt.Sprintf("SCENARIO %d/%d · Leader election: exactly one active scheduler", i, n),
		"3 scheduler replicas campaign for leadership under "+ElectionPath+".",
		"Only the leader runs the periodic job. Each standby watches only its predecessor.",
		"We crash the leader (takeover after session timeout), then stop the next one gracefully.")

	ActiveLeaders.Store(0)
	MaxActiveLeaders.Store(0)
	var scheds []*Scheduler
	for _, name := range []string{"scheduler-1", "scheduler-2", "scheduler-3"} {
		s := NewScheduler(d.start(name), d.Pause/2)
		scheds = append(scheds, s)
		go s.Run()
		sleepCtx(d.ctx, 100*time.Millisecond)
	}
	d.wait(2.5)

	leader := func() *Scheduler {
		var best *Scheduler
		for _, s := range scheds {
			if s.p.Ctx.Err() == nil && ActiveLeaders.Load() > 0 && (best == nil || s.Jobs() > best.Jobs()) {
				best = s
			}
		}
		return best
	}
	first := leader()
	d.Sink.Step("The leader (%s) CRASHES. Its election znode stays until its session expires, then the next replica takes over", first.p.Name)
	before := first.Jobs()
	first.p.Crash()
	ok := d.waitUntil(3*d.Env.SessionTimeout, func() bool {
		for _, s := range scheds {
			if s != first && s.Jobs() > 0 {
				return true
			}
		}
		return false
	})
	d.check(ok, "a standby replica took over after the crash (old leader ran %d jobs)", before)
	d.wait(2)

	var second *Scheduler
	for _, s := range scheds {
		if s != first && s.Jobs() > 0 {
			second = s
		}
	}
	if second != nil {
		d.Sink.Step("%s is shut down for maintenance (graceful): it resigns and the last replica takes over immediately", second.p.Name)
		second.p.Stop()
		d.wait(2)
	}
	total := int64(0)
	for _, s := range scheds {
		d.Sink.Println(fmt.Sprintf("     %s ran %d jobs", s.p.Name, s.Jobs()))
		total += s.Jobs()
	}
	d.check(MaxActiveLeaders.Load() == 1, "never more than one active leader at a time (max observed: %d)", MaxActiveLeaders.Load())
	d.check(total > 0, "jobs kept running across two leader changes (%d in total)", total)
	d.tree("/apps/scheduler")
	return nil
}

// ---------------------------------------------------------------------------

func (d *Demo) lock(i, n int) error {
	d.Sink.Banner(fmt.Sprintf("SCENARIO %d/%d · Distributed lock: no overselling", i, n),
		"4 booking agents sell 12 concert tickets stored in a non-transactional JSON file.",
		"Each sale is read → process payment → write. First WITHOUT coordination (race),",
		"then WITH the ZooKeeper lock (queue of EPHEMERAL|SEQUENTIAL znodes, no herd effect).")

	store := filepath.Join(os.TempDir(), "zkapp-tickets.json")
	agents := []string{"agent-1", "agent-2", "agent-3", "agent-4"}
	run := func(useLock bool) (sold int, st *TicketStore) {
		InitTickets(store, 12)
		var wg sync.WaitGroup
		var list []*TicketAgent
		for _, name := range agents {
			suffix := "-nolock"
			if useLock {
				suffix = "-locked"
			}
			a := NewTicketAgent(d.start(name+suffix), store, useLock)
			list = append(list, a)
			wg.Add(1)
			go func() { defer wg.Done(); a.Run() }()
		}
		wg.Wait()
		for _, a := range list {
			sold += a.Sold
		}
		st, _ = LoadTickets(store)
		return sold, st
	}

	d.Sink.Step("Round 1: agents sell WITHOUT the lock")
	sold, st := run(false)
	d.Sink.Println(fmt.Sprintf("     agents believe they sold %d tickets; store says remaining=%d with %d sale records", sold, st.Remaining, len(st.Sold)))
	d.check(true, "without a lock: %d tickets sold for a stock of 12 → %d OVERSOLD, %d sale records lost (expected failure)",
		sold, max(0, sold-12), sold-len(st.Sold))
	d.stopAll()
	d.wait(1)

	d.Sink.Step("Round 2: the same agents WITH the ZooKeeper lock")
	sold, st = run(true)
	d.Sink.Println(fmt.Sprintf("     agents sold %d tickets; store says remaining=%d with %d sale records", sold, st.Remaining, len(st.Sold)))
	d.check(sold == 12 && st.Remaining == 0 && len(st.Sold) == 12, "with the lock: exactly 12 sold, 0 remaining, every sale recorded")
	return nil
}

// ---------------------------------------------------------------------------

func (d *Demo) queue(i, n int) error {
	d.Sink.Banner(fmt.Sprintf("SCENARIO %d/%d · Fault-tolerant work queue", i, n),
		"A producer enqueues 8 jobs (SEQUENTIAL znodes). 3 workers claim jobs by creating an",
		"EPHEMERAL znode under "+ClaimsPath+" — create is atomic, so one worker wins.",
		"worker-2 crashes in the middle of its 2nd job: its claim expires and another worker redoes it.")

	producer := d.start("producer")
	if err := Produce(producer.Client, 8, "transcode video"); err != nil {
		return err
	}
	var workers []*Worker
	for _, name := range []string{"worker-1", "worker-2", "worker-3"} {
		w := NewWorker(d.start(name), d.Pause/2)
		if name == "worker-2" {
			w.OnClaim = func(task string, n int) {
				if n == 2 {
					d.Sink.Step("worker-2 CRASHES while processing %s (its claim stays until the session expires)", task)
					w.p.Crash()
				}
			}
		}
		workers = append(workers, w)
		go w.Run()
	}
	ok := d.waitUntil(30*time.Second, func() bool {
		pending, done, err := QueueStatus(producer.Client)
		return err == nil && len(pending) == 0 && len(done) == 8
	})
	pending, done, _ := QueueStatus(producer.Client)
	for _, w := range workers {
		d.Sink.Println(fmt.Sprintf("     %s completed %d jobs", w.p.Name, w.Done()))
	}
	d.check(ok, "all 8 jobs completed (%d done, %d pending), including the one abandoned by the crashed worker", len(done), len(pending))
	d.tree("/apps/queue")
	return nil
}

// ---------------------------------------------------------------------------

func (d *Demo) failover(i, n int) error {
	d.Sink.Banner(fmt.Sprintf("SCENARIO %d/%d · ZooKeeper server failure", i, n),
		"The applications keep running while the ZooKeeper LEADER SERVER crashes:",
		"followers detect it, elect a new leader (Zab), clients move their sessions to",
		"another server (with their watches), and ephemeral znodes survive. Then the old",
		"server comes back and catches up with a DIFF.")

	for _, name := range []string{"api-1", "api-2"} {
		if _, err := StartServiceInstance(d.start(name), "api"); err != nil {
			return err
		}
	}
	gw := NewGateway(d.start("gateway"), "api")
	go gw.Run(d.Pause / 4)
	cfgSvc := NewConfigurableService(d.start("shop-1"))
	go cfgSvc.Run()
	admin := d.start("operator")
	d.wait(2)

	old := d.Ensemble.Leader()
	d.Sink.Step("💥 Killing ZooKeeper server-%d (the current LEADER)", old)
	okBefore, _ := gw.Stats()
	d.Ensemble.StopServer(old)
	ok := d.Ensemble.WaitReady(15 * time.Second)
	d.check(ok, "remaining servers elected a new leader: server-%d", d.Ensemble.Leader())
	d.wait(1)

	d.Sink.Step("Writes keep working: the operator updates the config through the new leader")
	_, err := SetConfig(admin.Client, Settings{"failoverTest": time.Now().Format("15:04:05")})
	d.check(err == nil, "config update after failover succeeded")
	ok = d.waitUntil(5*time.Second, func() bool { cur, _ := cfgSvc.Current(); return cur["failoverTest"] != "" })
	d.check(ok, "the service's watch survived the session move and delivered the change")
	d.wait(2)
	okAfter, _ := gw.Stats()
	d.check(okAfter > okBefore && len(gw.Instances()) == 2,
		"gateway kept routing (%d requests after the crash) and both instances stayed registered", okAfter-okBefore)

	d.Sink.Step("Restarting server-%d: it rejoins as a follower and catches up", old)
	d.Ensemble.StartServer(old)
	ok = d.Ensemble.WaitReady(15 * time.Second)
	d.check(ok, "server-%d is back in the ensemble", old)
	ok = d.waitUntil(5*time.Second, func() bool {
		var digest [32]byte
		first := true
		for _, id := range d.Ensemble.IDs() {
			s := d.Ensemble.Server(id)
			if s == nil {
				return false
			}
			if first {
				digest, first = s.Digest(), false
			} else if s.Digest() != digest {
				return false
			}
		}
		return true
	})
	d.check(ok, "all replicas hold identical data again")
	return nil
}
