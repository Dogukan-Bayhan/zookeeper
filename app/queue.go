package app

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"zookeeper/client"
	"zookeeper/recipes"
	"zookeeper/zk"
)

// Scenario 5: fault-tolerant work queue.
//
// Problem: a pool of workers processes jobs (video transcoding, emails,
// imports). Each job must be processed by exactly one worker at a time, and
// a job taken by a worker that crashes must not be lost.
//
// Solution: producers add jobs as SEQUENTIAL znodes under /tasks. A worker
// claims a job by creating an EPHEMERAL znode /claims/<task>: create is
// atomic, so exactly one worker wins. When done, the worker records the
// result and deletes the task. If a worker crashes, its session expires,
// its ephemeral claim vanishes, the other workers (watching /claims) are
// notified and the job is picked up again.

const (
	TasksPath  = "/apps/queue/tasks"
	ClaimsPath = "/apps/queue/claims"
	DonePath   = "/apps/queue/done"
)

func ensureQueue(c *client.Client) error {
	for _, p := range []string{TasksPath, ClaimsPath, DonePath} {
		if err := recipes.EnsurePath(c, p); err != nil {
			return err
		}
	}
	return nil
}

// Produce enqueues n jobs.
func Produce(c *client.Client, n int, label string) error {
	if err := ensureQueue(c); err != nil {
		return err
	}
	lg := c.Logger().With("comp", "producer")
	for i := 1; i <= n; i++ {
		payload := fmt.Sprintf("%s #%d", label, i)
		p, err := c.Create(TasksPath+"/task-", []byte(payload), zk.FlagSequential)
		if err != nil {
			return err
		}
		lg.Info("📥 job enqueued", "task", zk.Base(p), "payload", payload)
	}
	return nil
}

// Worker processes jobs.
type Worker struct {
	p        *Process
	WorkTime time.Duration
	// OnClaim, if set, is called after a job is claimed (the demo uses it to
	// crash a worker in the middle of a job).
	OnClaim func(task string, n int)
	done    atomic.Int64
}

// NewWorker creates a worker.
func NewWorker(p *Process, workTime time.Duration) *Worker {
	return &Worker{p: p, WorkTime: workTime}
}

// Done returns the number of jobs completed by this worker.
func (w *Worker) Done() int64 { return w.done.Load() }

// Run processes jobs until the process stops.
func (w *Worker) Run() error {
	c, lg := w.p.Client, w.p.Log
	if err := ensureQueue(c); err != nil {
		return err
	}
	lg.Info("worker started, looking for jobs")
	claimed := 0
	for w.p.Ctx.Err() == nil {
		tasks, tasksCh, err := c.GetChildrenW(TasksPath)
		if err != nil {
			return ignoreStop(w.p.Ctx, err)
		}
		_, claimsCh, err := c.GetChildrenW(ClaimsPath)
		if err != nil {
			return ignoreStop(w.p.Ctx, err)
		}
		didWork := false
		for _, t := range tasks {
			if w.p.Ctx.Err() != nil {
				return nil
			}
			claim := ClaimsPath + "/" + t
			if _, err := c.Create(claim, []byte(w.p.Name), zk.FlagEphemeral); errors.Is(err, zk.ErrNodeExists) {
				lg.Debug("job already claimed by another worker, skipping", "task", t)
				continue
			} else if errors.Is(err, zk.ErrNoNode) {
				continue
			} else if err != nil {
				return ignoreStop(w.p.Ctx, err)
			}
			if st, _ := c.Exists(TasksPath+"/"+t, false); st == nil {
				c.Delete(claim, zk.AnyVersion) // finished by someone else meanwhile
				continue
			}
			claimed++
			payload, _, _ := c.GetData(TasksPath+"/"+t, false)
			lg.Info("🔧 claimed job (ephemeral claim znode)", "task", t, "payload", string(payload), "claim", claim)
			if w.OnClaim != nil {
				w.OnClaim(t, claimed)
			}
			if !sleepCtx(w.p.Ctx, w.WorkTime) {
				return nil // crashed or stopped in the middle of the job
			}
			result := fmt.Sprintf("%s processed by %s", payload, w.p.Name)
			if _, err := c.Create(DonePath+"/"+t, []byte(result), zk.FlagPersistent); err != nil && !errors.Is(err, zk.ErrNodeExists) {
				return ignoreStop(w.p.Ctx, err)
			}
			c.Delete(TasksPath+"/"+t, zk.AnyVersion)
			c.Delete(claim, zk.AnyVersion)
			w.done.Add(1)
			lg.Info("✅ job completed", "task", t, "result", result)
			didWork = true
		}
		if didWork {
			continue
		}
		lg.Info("no unclaimed job: waiting for new jobs or for a claim to disappear", "pending", len(tasks))
		select {
		case <-w.p.Ctx.Done():
			return nil
		case <-tasksCh:
			lg.Debug("job list changed")
		case <-claimsCh:
			lg.Info("claims changed (a job may have been abandoned by a crashed worker), re-scanning")
		}
	}
	return nil
}

func ignoreStop(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, zk.ErrClosed) {
		return nil
	}
	return err
}

// QueueStatus returns the number of pending and completed jobs.
func QueueStatus(c *client.Client) (pending, done []string, err error) {
	if err := c.Sync(TasksPath); err != nil {
		return nil, nil, err
	}
	if pending, err = c.GetChildren(TasksPath, false); err != nil {
		return
	}
	done, err = c.GetChildren(DonePath, false)
	return
}
