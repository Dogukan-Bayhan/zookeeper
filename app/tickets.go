package app

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"zookeeper/recipes"
)

// Scenario 4: distributed lock guarding a non-transactional resource.
//
// Problem: several booking agents sell tickets from a shared stock kept in
// a legacy store with no transactions (here: a JSON file). Each sale is a
// read-modify-write that takes some time. Without coordination, agents read
// the same stock, both sell "the last ticket", and the store is oversold.
//
// Solution: wrap the critical section in a ZooKeeper lock without herd
// effect (Section 2.4): agents queue with EPHEMERAL|SEQUENTIAL znodes, each
// waits only for its predecessor, and a crashed agent releases the lock
// automatically when its session ends.

// LockPath is the lock znode for the ticket store.
const LockPath = "/apps/tickets/lock"

// TicketStore is the shared, non-transactional resource.
type TicketStore struct {
	Initial   int      `json:"initial"`
	Remaining int      `json:"remaining"`
	Sold      []string `json:"sold"`
}

// InitTickets resets the store.
func InitTickets(path string, n int) error {
	return saveStore(path, &TicketStore{Initial: n, Remaining: n})
}

// LoadTickets reads the store.
func LoadTickets(path string) (*TicketStore, error) {
	var lastErr error
	for i := 0; i < 50; i++ { // a concurrent writer may be mid-write without a lock
		data, err := os.ReadFile(path)
		if err == nil {
			var st TicketStore
			if err = json.Unmarshal(data, &st); err == nil {
				return &st, nil
			}
		}
		lastErr = err
		time.Sleep(2 * time.Millisecond)
	}
	return nil, lastErr
}

func saveStore(path string, st *TicketStore) error {
	data, _ := json.MarshalIndent(st, "", "  ")
	return os.WriteFile(path, data, 0o644)
}

// TicketAgent sells tickets.
type TicketAgent struct {
	p       *Process
	store   string
	useLock bool
	// ProcessingTime simulates payment / validation inside the critical section.
	ProcessingTime time.Duration
	Sold           int
}

// NewTicketAgent creates an agent; useLock selects whether the ZooKeeper
// lock protects the read-modify-write.
func NewTicketAgent(p *Process, store string, useLock bool) *TicketAgent {
	return &TicketAgent{p: p, store: store, useLock: useLock, ProcessingTime: 30 * time.Millisecond}
}

// Run sells until the stock is exhausted or the process stops.
func (a *TicketAgent) Run() error {
	lg := a.p.Log
	lock := recipes.NewLock(a.p.Client, LockPath)
	mode := "WITH ZooKeeper lock"
	if !a.useLock {
		mode = "WITHOUT lock (unsafe)"
	}
	lg.Info("booking agent started", "mode", mode, "store", a.store)
	for i := 1; a.p.Ctx.Err() == nil; i++ {
		if a.useLock {
			if err := lock.Lock(a.p.Ctx); err != nil {
				return nil
			}
		}
		st, err := LoadTickets(a.store)
		if err != nil {
			if a.useLock {
				lock.Unlock()
			}
			return err
		}
		if st.Remaining <= 0 {
			lg.Info("sold out, stopping", "soldByMe", a.Sold)
			if a.useLock {
				lock.Unlock()
			}
			return nil
		}
		lg.Debug("read stock, processing payment...", "remaining", st.Remaining)
		time.Sleep(a.ProcessingTime) // other agents may run meanwhile
		st.Remaining--
		ticket := fmt.Sprintf("ticket-%02d→%s", st.Initial-st.Remaining, a.p.Name)
		st.Sold = append(st.Sold, ticket)
		if err := saveStore(a.store, st); err != nil {
			lg.Error("store write failed", "err", err)
		}
		a.Sold++
		lg.Info("🎟 sold a ticket", "ticket", ticket, "remainingAfter", st.Remaining)
		if a.useLock {
			lock.Unlock()
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}
