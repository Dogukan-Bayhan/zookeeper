package app

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"zookeeper/recipes"
	"zookeeper/zk"
)

// Scenario 3: leader election — exactly one active job scheduler.
//
// Problem: periodic jobs (billing runs, report generation, cleanup) must run
// exactly once, yet the scheduler must be highly available. Running several
// schedulers makes jobs run several times; running one makes it a single
// point of failure.
//
// Solution: run several schedulers; they elect a leader with
// EPHEMERAL|SEQUENTIAL znodes (lowest sequence wins, each candidate watches
// only its predecessor). Only the leader runs jobs. When the leader dies its
// znode disappears and the next candidate takes over automatically. The
// leader also stops running jobs as soon as it loses its connection to
// ZooKeeper, since it can no longer be sure it is still the leader.

const (
	ElectionPath = "/apps/scheduler/election"
	LastRunPath  = "/apps/scheduler/last-run"
)

// ActiveLeaders counts schedulers currently running jobs in this process
// (used by the demo to check that there is never more than one).
var ActiveLeaders atomic.Int32

// MaxActiveLeaders records the highest value ActiveLeaders reached.
var MaxActiveLeaders atomic.Int32

// Scheduler is a highly available job scheduler.
type Scheduler struct {
	p        *Process
	interval time.Duration
	jobs     atomic.Int64
}

// NewScheduler creates a scheduler running a job every interval while it
// leads.
func NewScheduler(p *Process, interval time.Duration) *Scheduler {
	return &Scheduler{p: p, interval: interval}
}

// Jobs returns how many jobs this scheduler ran.
func (s *Scheduler) Jobs() int64 { return s.jobs.Load() }

// Run campaigns for leadership and runs jobs while leading.
func (s *Scheduler) Run() error {
	c, lg := s.p.Client, s.p.Log
	if err := recipes.EnsurePath(c, zk.Parent(LastRunPath)); err != nil {
		return err
	}
	if leader, err := recipes.Leader(c, ElectionPath); err == nil {
		lg.Info("scheduler started in STANDBY; current leader is", "leader", string(leader))
	} else {
		lg.Info("scheduler started; no leader yet")
	}
	e := recipes.NewElection(c, ElectionPath)
	if err := e.Campaign(s.p.Ctx, []byte(s.p.Name)); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	lg.Info("👑 I am the LEADER now: starting to run scheduled jobs", "node", e.Node())
	n := ActiveLeaders.Add(1)
	for {
		m := MaxActiveLeaders.Load()
		if n <= m || MaxActiveLeaders.CompareAndSwap(m, n) {
			break
		}
	}
	defer ActiveLeaders.Add(-1)

	// Our candidate znode disappears if our session ends: watch it.
	_, lost, err := c.ExistsW(e.Node())
	if err != nil {
		return err
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.p.Ctx.Done():
			lg.Info("scheduler shutting down, giving up leadership")
			e.Resign()
			return nil
		case ev := <-lost:
			lg.Warn("leadership lost (our election znode changed)", "event", ev.String())
			return nil
		case <-ticker.C:
		}
		if st := c.State(); st != zk.StateConnected {
			lg.Warn("⏸ not connected to ZooKeeper: pausing jobs (cannot be sure we are still the leader)", "state", st.String())
			continue
		}
		job := s.jobs.Add(1)
		record := fmt.Sprintf("%s ran job #%d at %s", s.p.Name, job, time.Now().Format("15:04:05.000"))
		lg.Info("⚙ running scheduled job (only the leader does this)", "job", job, "task", "nightly-report")
		if _, err := c.SetData(LastRunPath, []byte(record), zk.AnyVersion); errors.Is(err, zk.ErrNoNode) {
			c.Create(LastRunPath, []byte(record), zk.FlagPersistent)
		}
	}
}
