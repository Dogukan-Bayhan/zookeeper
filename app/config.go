package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"zookeeper/client"
	"zookeeper/recipes"
	"zookeeper/zk"
)

// Scenario 1: dynamic configuration.
//
// Problem: a fleet of service instances must change settings (rate limits,
// feature flags, log levels) at runtime, without a redeploy or restart, and
// every instance must converge to the same configuration.
//
// Solution: the configuration lives in one znode. Every instance reads it
// with a watch; when an operator updates it, ZooKeeper notifies every
// instance, which re-reads the znode and re-arms the watch (Section 2.4,
// "Configuration Management"). Operators update with a conditional
// setData on the version they read, so two concurrent edits never silently
// overwrite each other.

// ConfigPath is the znode holding the shop configuration.
const ConfigPath = "/apps/shop/config"

// Settings is the configuration document.
type Settings map[string]string

// SetConfig merges updates into the configuration. It is a read-modify-write
// protected by the znode version: if someone else changed the config in
// between, the write fails with ErrBadVersion and we retry on fresh data.
func SetConfig(c *client.Client, updates Settings) (int32, error) {
	lg := c.Logger().With("comp", "config-admin")
	if err := recipes.EnsurePath(c, zk.Parent(ConfigPath)); err != nil {
		return 0, err
	}
	for attempt := 1; ; attempt++ {
		data, st, err := c.GetData(ConfigPath, false)
		if errors.Is(err, zk.ErrNoNode) {
			body, _ := json.Marshal(updates)
			if _, err := c.Create(ConfigPath, body, zk.FlagPersistent); errors.Is(err, zk.ErrNodeExists) {
				continue
			} else if err != nil {
				return 0, err
			}
			lg.Info("★ configuration created", "path", ConfigPath, "config", string(body))
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		cur := Settings{}
		json.Unmarshal(data, &cur)
		for k, v := range updates {
			cur[k] = v
		}
		body, _ := json.Marshal(cur)
		lg.Info("writing new configuration (conditional on the version we read)",
			"expectedVersion", st.Version, "updates", fmtSettings(updates), "attempt", attempt)
		nst, err := c.SetData(ConfigPath, body, st.Version)
		if errors.Is(err, zk.ErrBadVersion) {
			lg.Warn("⚠ concurrent update detected (version changed since our read): re-reading and retrying",
				"staleVersion", st.Version)
			continue
		}
		if err != nil {
			return 0, err
		}
		lg.Info("★ configuration updated", "newVersion", nst.Version, "config", fmtSettings(cur))
		return nst.Version, nil
	}
}

// ConfigurableService is a service instance that applies the configuration
// live.
type ConfigurableService struct {
	p       *Process
	mu      sync.Mutex
	current Settings
	version int32
}

// NewConfigurableService wraps a process.
func NewConfigurableService(p *Process) *ConfigurableService {
	return &ConfigurableService{p: p, version: -1}
}

// Current returns the configuration currently applied and its version.
func (s *ConfigurableService) Current() (Settings, int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current, s.version
}

// Run watches the configuration until the process stops.
func (s *ConfigurableService) Run() error {
	lg := s.p.Log
	lg.Info("service started: loading configuration and watching for changes", "path", ConfigPath)
	err := recipes.WatchConfig(s.p.Ctx, s.p.Client, ConfigPath, func(data []byte, st *zk.Stat) {
		next := Settings{}
		if err := json.Unmarshal(data, &next); err != nil {
			lg.Error("invalid configuration, keeping the previous one", "err", err)
			return
		}
		s.mu.Lock()
		prev := s.current
		s.current, s.version = next, st.Version
		s.mu.Unlock()
		changes := diffSettings(prev, next)
		if len(changes) == 0 {
			changes = []string{"(no change)"}
		}
		lg.Info("✅ configuration applied live, no restart needed", "version", st.Version, "changes", fmt.Sprint(changes))
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func diffSettings(a, b Settings) []string {
	var out []string
	keys := map[string]bool{}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		av, aok := a[k]
		bv, bok := b[k]
		switch {
		case !aok:
			out = append(out, fmt.Sprintf("+%s=%s", k, bv))
		case !bok:
			out = append(out, fmt.Sprintf("-%s", k))
		case av != bv:
			out = append(out, fmt.Sprintf("%s:%s→%s", k, av, bv))
		}
	}
	return out
}

func fmtSettings(s Settings) string {
	var keys []string
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, k+"="+s[k])
	}
	return "{" + joinComma(parts) + "}"
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}
