package recipes

import (
	"context"
	"errors"
	"slices"

	"zookeeper/client"
	"zookeeper/zk"
)

// ---------------------------------------------------------------------------
// Simple locks ("lock files")

// SimpleLock is the simplest lock: the lock is a single ephemeral znode.
// It suffers from the herd effect: on release, every waiter wakes up and
// races for the lock.
type SimpleLock struct {
	c    *client.Client
	path string
}

// NewSimpleLock returns a lock represented by znode path.
func NewSimpleLock(c *client.Client, path string) *SimpleLock {
	return &SimpleLock{c: c, path: path}
}

// Lock acquires the lock.
func (l *SimpleLock) Lock(ctx context.Context) error {
	for {
		_, err := l.c.Create(l.path, nil, zk.FlagEphemeral)
		if err == nil {
			return nil
		}
		if !errors.Is(err, zk.ErrNodeExists) {
			return err
		}
		st, ch, err := l.c.ExistsW(l.path)
		if err != nil {
			return err
		}
		if st == nil {
			continue
		}
		if err := wait(ctx, ch); err != nil {
			return err
		}
	}
}

// Unlock releases the lock.
func (l *SimpleLock) Unlock() error {
	return l.c.Delete(l.path, zk.AnyVersion)
}

// ---------------------------------------------------------------------------
// Locks without herd effect

// Lock is the lock of Section 2.4 without herd effect. Clients line up by
// creating EPHEMERAL|SEQUENTIAL znodes under the lock znode l; each client
// watches only the znode just before its own, so a release wakes a single
// client. Crashed clients release their requests automatically.
//
//	Lock
//	1 n = create(l + "/lock-", EPHEMERAL|SEQUENTIAL)
//	2 C = getChildren(l, false)
//	3 if n is lowest znode in C, exit
//	4 p = znode in C ordered just before n
//	5 if exists(p, true) wait for watch event
//	6 goto 2
//	Unlock
//	1 delete(n)
type Lock struct {
	c    *client.Client
	dir  string
	node string
}

// NewLock returns a lock rooted at znode dir.
func NewLock(c *client.Client, dir string) *Lock {
	return &Lock{c: c, dir: dir}
}

// Lock acquires the lock.
func (l *Lock) Lock(ctx context.Context) error {
	if l.node != "" {
		return errors.New("recipes: lock already held or requested")
	}
	node, err := enqueue(l.c, l.dir, "lock-")
	if err != nil {
		return err
	}
	l.node = node
	err = waitTurn(ctx, l.c, l.dir, node, func(nodes []seqNode, me int) int {
		return me - 1 // the znode just before ours, -1 if we are first
	}, "lock-")
	if err != nil {
		l.c.Delete(node, zk.AnyVersion)
		l.node = ""
	}
	return err
}

// Unlock releases the lock.
func (l *Lock) Unlock() error {
	if l.node == "" {
		return errors.New("recipes: lock not held")
	}
	err := l.c.Delete(l.node, zk.AnyVersion)
	l.node = ""
	return err
}

// Node returns the znode representing this lock request.
func (l *Lock) Node() string { return l.node }

func enqueue(c *client.Client, dir, prefix string) (string, error) {
	if err := EnsurePath(c, dir); err != nil {
		return "", err
	}
	return c.Create(zk.Join(dir, prefix), nil, zk.FlagEphemeral|zk.FlagSequential)
}

// waitTurn implements lines 2-6 of the lock procedures: blocker returns the
// index of the znode that must go away before we may proceed, or -1.
func waitTurn(ctx context.Context, c *client.Client, dir, node string,
	blocker func(nodes []seqNode, me int) int, prefixes ...string) error {
	name := zk.Base(node)
	for {
		children, err := c.GetChildren(dir, false)
		if err != nil {
			return err
		}
		nodes := sortedSeq(children, prefixes...)
		me := slices.IndexFunc(nodes, func(n seqNode) bool { return n.name == name })
		if me < 0 {
			return errors.New("recipes: lock znode disappeared (session expired?)")
		}
		b := blocker(nodes, me)
		if b < 0 {
			return nil
		}
		st, ch, err := c.ExistsW(zk.Join(dir, nodes[b].name))
		if err != nil {
			return err
		}
		if st == nil {
			continue // already gone, re-check
		}
		if err := wait(ctx, ch); err != nil {
			return err
		}
	}
}

// ---------------------------------------------------------------------------
// Read/write locks

// RWLock is the read/write lock of Section 2.4. Write locks behave like
// Lock; a read lock only waits for earlier write requests, so readers share
// the lock. All readers waiting on the same writer are released together,
// which is the desired behavior.
type RWLock struct {
	c    *client.Client
	dir  string
	node string
}

// NewRWLock returns a read/write lock rooted at dir.
func NewRWLock(c *client.Client, dir string) *RWLock {
	return &RWLock{c: c, dir: dir}
}

// Lock acquires the write lock.
//
//	1 n = create(l + "/write-", EPHEMERAL|SEQUENTIAL)
//	2 C = getChildren(l, false)
//	3 if n is lowest znode in C, exit
//	4 p = znode in C ordered just before n
//	5 if exists(p, true) wait for event
//	6 goto 2
func (l *RWLock) Lock(ctx context.Context) error {
	return l.acquire(ctx, "write-", func(nodes []seqNode, me int) int { return me - 1 })
}

// RLock acquires a read lock.
//
//	1 n = create(l + "/read-", EPHEMERAL|SEQUENTIAL)
//	2 C = getChildren(l, false)
//	3 if no write znodes lower than n in C, exit
//	4 p = write znode in C ordered just before n
//	5 if exists(p, true) wait for event
//	6 goto 3
func (l *RWLock) RLock(ctx context.Context) error {
	return l.acquire(ctx, "read-", func(nodes []seqNode, me int) int {
		for i := me - 1; i >= 0; i-- {
			if len(nodes[i].name) >= 6 && nodes[i].name[:6] == "write-" {
				return i
			}
		}
		return -1
	})
}

func (l *RWLock) acquire(ctx context.Context, prefix string, blocker func([]seqNode, int) int) error {
	if l.node != "" {
		return errors.New("recipes: lock already held or requested")
	}
	node, err := enqueue(l.c, l.dir, prefix)
	if err != nil {
		return err
	}
	l.node = node
	if err := waitTurn(ctx, l.c, l.dir, node, blocker, "read-", "write-"); err != nil {
		l.c.Delete(node, zk.AnyVersion)
		l.node = ""
		return err
	}
	return nil
}

// Unlock releases a read or write lock.
func (l *RWLock) Unlock() error {
	if l.node == "" {
		return errors.New("recipes: lock not held")
	}
	err := l.c.Delete(l.node, zk.AnyVersion)
	l.node = ""
	return err
}
