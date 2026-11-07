package app

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"zookeeper/transport"
	"zookeeper/zlog"
)

// TestDemoScenarios runs every application scenario end to end, including
// crashed processes and a crashed ZooKeeper leader. Set ZK_LOG=debug to see
// the logs.
func TestDemoScenarios(t *testing.T) {
	sink := zlog.NewSink(io.Discard, false)
	level := slog.LevelInfo
	if lv := os.Getenv("ZK_LOG"); lv != "" {
		sink = zlog.NewTerminalSink()
		level, _ = zlog.ParseLevel(lv)
	}
	net := transport.NewMemNetwork()
	ens, err := StartEnsemble(EnsembleConfig{
		Peers:     map[int64]string{1: "zk-1", 2: "zk-2", 3: "zk-3"},
		Transport: net, Logger: sink.Logger(level), Tick: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ens.Close()
	d := &Demo{
		Env: &Env{Servers: ens.Addrs(), Transport: net, Sink: sink, Level: level,
			SessionTimeout: time.Second},
		Ensemble: ens, Sink: sink, Pause: 300 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := d.Run(ctx, Scenarios); err != nil {
		t.Fatal(err)
	}
	if d.Failures() != 0 {
		t.Fatalf("%d demo checks failed (rerun with ZK_LOG=info to see them)", d.Failures())
	}
}
