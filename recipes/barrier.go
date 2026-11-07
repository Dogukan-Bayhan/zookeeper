package recipes

import (
	"context"
	"errors"
	"sort"

	"zookeeper/client"
	"zookeeper/zk"
)

// ---------------------------------------------------------------------------
// Double barrier

// DoubleBarrier synchronizes the beginning and the end of a computation.
// Processes register with barrier znode b by creating a child on Enter and
// remove it on Leave. They may start once the number of children reaches the
// threshold (signalled by a "ready" child created by the process that
// completed the group), and they may finish once every process removed its
// child. Watches are used to wait efficiently for both conditions.
type DoubleBarrier struct {
	c    *client.Client
	path string
	name string
	size int
}

const readyNode = "ready"

// NewDoubleBarrier returns a barrier at znode path for size processes. name
// must be unique among the participants.
func NewDoubleBarrier(c *client.Client, path, name string, size int) *DoubleBarrier {
	return &DoubleBarrier{c: c, path: path, name: name, size: size}
}

func (b *DoubleBarrier) participants() ([]string, error) {
	children, err := b.c.GetChildren(b.path, false)
	if err != nil {
		return nil, err
	}
	out := children[:0]
	for _, ch := range children {
		if ch != readyNode {
			out = append(out, ch)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Enter joins the barrier and blocks until enough processes have joined.
func (b *DoubleBarrier) Enter(ctx context.Context) error {
	if err := EnsurePath(b.c, b.path); err != nil {
		return err
	}
	lg := logger(b.c, "barrier")
	ready := zk.Join(b.path, readyNode)
	// Watch for "ready" before registering so the event cannot be missed.
	st, ch, err := b.c.ExistsW(ready)
	if err != nil {
		return err
	}
	if _, err := b.c.Create(zk.Join(b.path, b.name), nil, zk.FlagEphemeral); err != nil &&
		!errors.Is(err, zk.ErrNodeExists) {
		return err
	}
	for {
		if st != nil {
			lg.Info("barrier is ready: everyone arrived, starting", "barrier", b.path)
			return nil
		}
		members, err := b.participants()
		if err != nil {
			return err
		}
		if len(members) >= b.size {
			lg.Info("we completed the group: creating ready znode", "barrier", b.path, "members", len(members))
			if _, err := b.c.Create(ready, nil, zk.FlagPersistent); err != nil && !errors.Is(err, zk.ErrNodeExists) {
				return err
			}
			return nil
		}
		lg.Info("entered barrier, waiting for the others", "barrier", b.path, "arrived", len(members), "needed", b.size)
		if err := wait(ctx, ch); err != nil {
			return err
		}
		if st, ch, err = b.c.ExistsW(ready); err != nil {
			return err
		}
	}
}

// Leave removes the process from the barrier and blocks until every process
// has left. To avoid the herd effect only two znodes are watched: the lowest
// participant waits for the highest one, everyone else waits for the lowest.
func (b *DoubleBarrier) Leave(ctx context.Context) error {
	own := zk.Join(b.path, b.name)
	for {
		members, err := b.participants()
		if err != nil {
			return err
		}
		if len(members) == 0 {
			logger(b.c, "barrier").Info("everyone left the barrier", "barrier", b.path)
			return nil
		}
		if len(members) == 1 && members[0] == b.name {
			if err := b.c.Delete(own, zk.AnyVersion); err != nil && !errors.Is(err, zk.ErrNoNode) {
				return err
			}
			// Last one out resets the barrier for reuse.
			b.c.Delete(zk.Join(b.path, readyNode), zk.AnyVersion)
			return nil
		}
		var target string
		if members[0] == b.name {
			target = members[len(members)-1]
		} else {
			if err := b.c.Delete(own, zk.AnyVersion); err != nil && !errors.Is(err, zk.ErrNoNode) {
				return err
			}
			target = members[0]
		}
		st, ch, err := b.c.ExistsW(zk.Join(b.path, target))
		if err != nil {
			return err
		}
		if st == nil {
			continue
		}
		logger(b.c, "barrier").Info("leaving: waiting for others to finish", "barrier", b.path,
			"remaining", len(members), "watching", target)
		if err := wait(ctx, ch); err != nil {
			return err
		}
	}
}

// ---------------------------------------------------------------------------
// Leader election

// Election elects a leader among processes: each candidate creates an
// EPHEMERAL|SEQUENTIAL znode, the lowest one leads, and every other
// candidate watches only its predecessor (no herd effect). If the leader
// dies its znode vanishes and the next candidate takes over.
type Election struct {
	c    *client.Client
	dir  string
	node string
}

// NewElection returns an election rooted at dir.
func NewElection(c *client.Client, dir string) *Election {
	return &Election{c: c, dir: dir}
}

// Campaign blocks until this process is the leader. data is stored in the
// candidate znode (e.g. the leader's address).
func (e *Election) Campaign(ctx context.Context, data []byte) error {
	if e.node != "" {
		return errors.New("recipes: already campaigning")
	}
	if err := EnsurePath(e.c, e.dir); err != nil {
		return err
	}
	node, err := e.c.Create(zk.Join(e.dir, "n-"), data, zk.FlagEphemeral|zk.FlagSequential)
	if err != nil {
		return err
	}
	e.node = node
	err = waitTurn(ctx, e.c, "election", e.dir, node, func(_ []seqNode, me int) int { return me - 1 }, "n-")
	if err == nil {
		logger(e.c, "election").Info("👑 elected leader (lowest sequence number)", "node", node)
	}
	if err != nil {
		e.c.Delete(node, zk.AnyVersion)
		e.node = ""
	}
	return err
}

// Resign gives up leadership (or candidacy).
func (e *Election) Resign() error {
	if e.node == "" {
		return nil
	}
	logger(e.c, "election").Info("resigning leadership", "node", e.node)
	err := e.c.Delete(e.node, zk.AnyVersion)
	e.node = ""
	return err
}

// Node returns the candidate znode (empty if not campaigning).
func (e *Election) Node() string { return e.node }

// Leader returns the data of the current leader, or zk.ErrNoNode if there is
// none. It syncs first so that the answer is not stale.
func Leader(c *client.Client, dir string) ([]byte, error) {
	if err := c.Sync(dir); err != nil {
		return nil, err
	}
	for {
		children, err := c.GetChildren(dir, false)
		if err != nil {
			return nil, err
		}
		nodes := sortedSeq(children, "n-")
		if len(nodes) == 0 {
			return nil, zk.ErrNoNode
		}
		data, _, err := c.GetData(zk.Join(dir, nodes[0].name), false)
		if errors.Is(err, zk.ErrNoNode) {
			continue // leader just went away
		}
		return data, err
	}
}
