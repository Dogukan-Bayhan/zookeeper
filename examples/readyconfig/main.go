// Command readyconfig demonstrates the scenario of Section 2.3 of the paper
// on an in-process 3-server ensemble: a new leader must change many
// configuration znodes and processes must never use a partial configuration.
//
// The leader deletes the "ready" znode, updates the configuration with
// pipelined asynchronous requests and re-creates "ready". Thanks to FIFO
// client order and linearizable writes, a process that sees "ready" also
// sees every change made before it; and since a watch notification arrives
// before the new state is visible, a process that read "ready" with a watch
// learns that a change started before it can read any new value.
package main

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"zookeeper/client"
	"zookeeper/server"
	"zookeeper/transport"
	"zookeeper/zk"
)

const n = 2000

func main() {
	net := transport.NewMemNetwork()
	peers := map[int64]string{1: "zk1", 2: "zk2", 3: "zk3"}
	for id := range peers {
		s, err := server.New(server.Config{ID: id, Peers: peers, Transport: net, TickTime: 20 * time.Millisecond})
		if err != nil {
			log.Fatal(err)
		}
		if err := s.Start(); err != nil {
			log.Fatal(err)
		}
		defer s.Close()
	}
	connect := func() *client.Client {
		c, err := client.Connect(client.Config{Servers: []string{"zk1", "zk2", "zk3"}, Transport: net,
			SessionTimeout: 5 * time.Second, ConnectTimeout: 10 * time.Second})
		if err != nil {
			log.Fatal(err)
		}
		return c
	}
	leader, worker := connect(), connect()
	defer leader.Close()
	defer worker.Close()

	if _, err := leader.Create("/config", nil, zk.FlagPersistent); err != nil {
		log.Fatal(err)
	}

	// Synchronous updates: one round trip per znode.
	start := time.Now()
	for i := 0; i < n/10; i++ {
		if _, err := leader.Create(fmt.Sprintf("/config/s%05d", i), []byte("v0"), zk.FlagPersistent); err != nil {
			log.Fatal(err)
		}
	}
	syncRate := time.Since(start) / time.Duration(n/10)
	fmt.Printf("synchronous: %v per update (%d updates would take %v)\n", syncRate, n, syncRate*n)

	// Worker: waits for ready, then checks that the configuration is complete.
	done := make(chan string)
	go func() {
		for {
			st, ch, err := worker.ExistsW("/config/ready")
			if err != nil {
				log.Fatal(err)
			}
			if st == nil {
				<-ch
				continue
			}
			kids, err := worker.GetChildren("/config", false)
			if err != nil {
				log.Fatal(err)
			}
			done <- fmt.Sprintf("worker saw ready with %d config znodes (expected %d + ready)", len(kids)-n/10, n)
			return
		}
	}()

	// Leader: delete ready, pipeline the changes, create ready.
	start = time.Now()
	var wg sync.WaitGroup
	wg.Add(n + 2)
	leader.DeleteAsync("/config/ready", zk.AnyVersion, func(err error) {
		if err != nil && !errors.Is(err, zk.ErrNoNode) {
			log.Fatal(err)
		}
		wg.Done()
	})
	for i := 0; i < n; i++ {
		leader.CreateAsync(fmt.Sprintf("/config/p%05d", i), []byte("v1"), zk.FlagPersistent, func(_ string, err error) {
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		})
	}
	leader.CreateAsync("/config/ready", nil, zk.FlagPersistent, func(_ string, err error) {
		if err != nil {
			log.Fatal(err)
		}
		wg.Done()
	})
	wg.Wait()
	fmt.Printf("asynchronous: %d updates + ready in %v\n", n, time.Since(start))
	fmt.Println(<-done)
}
