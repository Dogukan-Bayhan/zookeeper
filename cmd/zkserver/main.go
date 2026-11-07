// Command zkserver runs one member of a ZooKeeper ensemble over TCP.
//
//	zkserver -id 1 -servers 1=127.0.0.1:2181,2=127.0.0.1:2182,3=127.0.0.1:2183 -dir data1
package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"zookeeper/server"
	"zookeeper/zlog"
)

func main() {
	id := flag.Int64("id", 1, "server id")
	servers := flag.String("servers", "1=127.0.0.1:2181", "comma separated id=host:port list of the ensemble")
	dir := flag.String("dir", "", "data directory (empty: in-memory, not durable)")
	tick := flag.Duration("tick", 100*time.Millisecond, "tick time")
	fsync := flag.Bool("fsync", true, "force log writes to disk before acknowledging")
	snapCount := flag.Int("snapcount", 10000, "transactions between snapshots")
	level := flag.String("log", "info", "log level: trace, debug, info, warn")
	flag.Parse()

	peers := map[int64]string{}
	for _, part := range strings.Split(*servers, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			log.Fatalf("bad server spec %q", part)
		}
		n, err := strconv.ParseInt(kv[0], 10, 64)
		if err != nil {
			log.Fatalf("bad server id %q", kv[0])
		}
		peers[n] = kv[1]
	}

	cfg := server.Config{ID: *id, Peers: peers, TickTime: *tick, SnapCount: *snapCount}
	if *dir != "" {
		st, err := server.NewFileStorage(*dir, *fsync)
		if err != nil {
			log.Fatal(err)
		}
		defer st.Close()
		cfg.Storage = st
	}
	lvl, err := zlog.ParseLevel(*level)
	if err != nil {
		log.Fatal(err)
	}
	logger := zlog.NewTerminal(lvl)
	slog.SetDefault(logger)
	cfg.Logger = logger
	s, err := server.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := s.Start(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("zkserver %d listening on %s (ensemble of %d, log level %s)\n", *id, peers[*id], len(peers), *level)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	s.Close()
}
