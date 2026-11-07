// Package recipes implements the coordination primitives of Section 2.4 of
// the ZooKeeper paper entirely on the client side, using only the client
// API: configuration management, rendezvous, group membership, simple locks,
// locks without herd effect, read/write locks, double barriers and leader
// election. The service knows nothing about these primitives.
package recipes

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"

	"zookeeper/client"
	"zookeeper/zk"
)

// logger returns the client's logger tagged with the recipe name.
func logger(c *client.Client, recipe string) *slog.Logger {
	return c.Logger().With("comp", recipe)
}

// EnsurePath creates path and its missing ancestors as regular znodes.
func EnsurePath(c *client.Client, path string) error {
	if path == "/" {
		return nil
	}
	if err := EnsurePath(c, zk.Parent(path)); err != nil {
		return err
	}
	_, err := c.Create(path, nil, zk.FlagPersistent)
	if err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return err
	}
	return nil
}

// wait blocks until a watch fires or the context is done.
func wait(ctx context.Context, ch <-chan zk.Event) error {
	select {
	case ev := <-ch:
		if ev.Type == zk.EventSession && ev.State == zk.StateExpired {
			return zk.ErrSessionExpired
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// seqNode is a sequential znode name with its sequence number.
type seqNode struct {
	name string
	seq  int64
}

// sortedSeq returns the sequential children with the given prefixes, ordered
// by sequence number.
func sortedSeq(children []string, prefixes ...string) []seqNode {
	var out []seqNode
	for _, ch := range children {
		for _, p := range prefixes {
			if strings.HasPrefix(ch, p) {
				if n, ok := zk.SequenceOf(ch); ok {
					out = append(out, seqNode{ch, n})
				}
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out
}

// ---------------------------------------------------------------------------
// Configuration management

// WatchConfig reads the configuration stored in znode zc and calls onChange
// with every new version, re-setting the watch each time. If zc does not
// exist yet it waits for its creation. Intermediate updates may be skipped:
// a notification only says the cached copy is stale. It returns when the
// context is done.
func WatchConfig(ctx context.Context, c *client.Client, zc string, onChange func(data []byte, st *zk.Stat)) error {
	lg := logger(c, "config")
	for {
		data, st, ch, err := c.GetDataW(zc)
		if errors.Is(err, zk.ErrNoNode) {
			lg.Info("config znode does not exist yet, waiting for its creation", "path", zc)
			st, ech, err := c.ExistsW(zc)
			if err != nil {
				return err
			}
			if st == nil {
				if err := wait(ctx, ech); err != nil {
					return err
				}
			}
			continue
		}
		if err != nil {
			return err
		}
		lg.Info("config read, watch set for the next change", "path", zc, "version", st.Version, "bytes", len(data))
		onChange(data, st)
		if err := wait(ctx, ch); err != nil {
			return err
		}
		lg.Info("config change notification: cached copy is stale, re-reading", "path", zc)
	}
}

// ---------------------------------------------------------------------------
// Rendezvous

// Rendezvous lets processes whose final configuration is not known in
// advance meet through a znode zr: the master fills it in, the workers wait
// for it.
type Rendezvous struct {
	c    *client.Client
	path string
}

// NewRendezvous creates (if needed) the rendezvous znode. With ephemeral the
// znode disappears with the creating session, so workers can clean up when
// the owner goes away.
func NewRendezvous(c *client.Client, path string, ephemeral bool) (*Rendezvous, error) {
	flags := zk.FlagPersistent
	if ephemeral {
		flags = zk.FlagEphemeral
	}
	if _, err := c.Create(path, nil, flags); err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return nil, err
	}
	return &Rendezvous{c: c, path: path}, nil
}

// OpenRendezvous returns a handle to an existing rendezvous znode.
func OpenRendezvous(c *client.Client, path string) *Rendezvous {
	return &Rendezvous{c: c, path: path}
}

// Publish fills in the rendezvous information (master side).
func (r *Rendezvous) Publish(info []byte) error {
	_, err := r.c.SetData(r.path, info, zk.AnyVersion)
	if err == nil {
		logger(r.c, "rendezvous").Info("published rendezvous information", "path", r.path, "info", string(info))
	}
	return err
}

// Wait returns the rendezvous information once it has been published
// (worker side).
func (r *Rendezvous) Wait(ctx context.Context) ([]byte, error) {
	for {
		data, _, ch, err := r.c.GetDataW(r.path)
		if errors.Is(err, zk.ErrNoNode) {
			// Not created yet (or not yet visible at our server).
			st, ech, err := r.c.ExistsW(r.path)
			if err != nil {
				return nil, err
			}
			if st == nil {
				if err := wait(ctx, ech); err != nil {
					return nil, err
				}
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(data) > 0 {
			logger(r.c, "rendezvous").Info("rendezvous information available", "path", r.path, "info", string(data))
			return data, nil
		}
		logger(r.c, "rendezvous").Info("rendezvous not filled in yet, waiting on watch", "path", r.path)
		if err := wait(ctx, ch); err != nil {
			return nil, err
		}
	}
}

// ---------------------------------------------------------------------------
// Group membership

// JoinGroup adds the process to group zg by creating an ephemeral child. If
// name is empty a unique name is assigned with the SEQUENTIAL flag. The
// member disappears automatically when the session ends. It returns the
// path of the member znode.
func JoinGroup(c *client.Client, zg, name string, info []byte) (string, error) {
	var p string
	var err error
	if name == "" {
		p, err = c.Create(zk.Join(zg, "member-"), info, zk.FlagEphemeral|zk.FlagSequential)
	} else {
		p, err = c.Create(zk.Join(zg, name), info, zk.FlagEphemeral)
	}
	if err == nil {
		logger(c, "group").Info("joined group with an ephemeral znode (removed automatically if we die)", "member", p)
	}
	return p, err
}

// Members lists the members of a group.
func Members(c *client.Client, zg string) ([]string, error) {
	return c.GetChildren(zg, false)
}

// WatchMembers calls onChange with the membership of zg every time it
// changes, until the context is done.
func WatchMembers(ctx context.Context, c *client.Client, zg string, onChange func([]string)) error {
	for {
		members, ch, err := c.GetChildrenW(zg)
		if err != nil {
			return err
		}
		logger(c, "group").Debug("membership read, watching for changes", "group", zg, "members", len(members))
		onChange(members)
		if err := wait(ctx, ch); err != nil {
			return err
		}
	}
}
