// Command zkapp runs real-world applications on top of the ZooKeeper
// library, with detailed logs of every step.
//
// All-in-one demo (in-process ensemble, failures injected):
//
//	zkapp demo                 # every scenario
//	zkapp demo discovery lock  # some scenarios
//
// Multi-terminal mode (each command is a separate process):
//
//	zkapp cluster                                  # terminal 1: 3 servers on 127.0.0.1:2181-2183
//	zkapp service -service api -name api-1         # terminal 2, 3...: HTTP instances
//	zkapp gateway -service api                     # terminal 4: load balancer
//	zkapp config-service -name shop-1              # live configuration
//	zkapp config-set rateLimit=50 feature.x=on
//	zkapp scheduler -name scheduler-1              # leader election
//	zkapp tickets-init -count 20 / zkapp tickets-agent -name a1 [-nolock]
//	zkapp queue-produce -count 10 / zkapp queue-worker -name w1
//	zkapp tree /apps
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"zookeeper/app"
	"zookeeper/transport"
	"zookeeper/web"
	"zookeeper/zlog"
)

const defaultServers = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

func usage() {
	fmt.Fprint(os.Stderr, `zkapp — applications built on ZooKeeper, with step-by-step logs

WEB LAB (interactive UI: scenarios, live logs per server, replica trees, crash buttons):
  zkapp web [-addr 127.0.0.1:8080] [-n 3] [-echo]   then open http://127.0.0.1:8080

ALL-IN-ONE DEMO (in-process 3-server ensemble, failures injected):
  zkapp demo [scenario...]        scenarios: config discovery election lock queue failover (default: all)
      -log debug                  level of application/client logs (trace|debug|info)
      -server-log info            level of ZooKeeper server logs (use debug to see every PROPOSAL/ACK/COMMIT)
      -pause 1.5s                 pause between steps

MULTI-TERMINAL MODE (start the cluster, then one command per terminal):
  zkapp cluster [-n 3] [-port 2181] [-dir DIR] [-log info]
  zkapp service        -service api -name api-1     HTTP instance registered in /services/api
  zkapp gateway        -service api                 discovers instances and load-balances requests
  zkapp config-service -name shop-1                 applies /apps/shop/config live
  zkapp config-set     key=value [key=value...]     updates the configuration
  zkapp scheduler      -name scheduler-1            leader election: only the leader runs jobs
  zkapp tickets-init   -count 20                    resets the shared ticket store
  zkapp tickets-agent  -name agent-1 [-nolock]      sells tickets under the ZooKeeper lock
  zkapp queue-produce  -count 10                    enqueues jobs
  zkapp queue-worker   -name worker-1               processes jobs
  zkapp tree           [/path]                      prints the znode tree

Common flags: -servers `+defaultServers+`  -log debug  -timeout 4s
Press Ctrl+C to stop a process gracefully (its session closes, ephemeral znodes vanish at once).
Close the terminal window / kill the process to simulate a crash (removal after the session timeout).
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var err error
	switch cmd {
	case "demo":
		err = runDemo(ctx, args)
	case "cluster":
		err = runCluster(ctx, args)
	case "web":
		err = runWeb(ctx, args)
	case "service", "gateway", "config-service", "config-set", "scheduler",
		"tickets-init", "tickets-agent", "queue-produce", "queue-worker", "tree":
		err = runApp(ctx, cmd, args)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func level(s string) slog.Level {
	l, err := zlog.ParseLevel(s)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return l
}

// ---------------------------------------------------------------------------

func runDemo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	logLevel := fs.String("log", "debug", "application and client log level")
	serverLevel := fs.String("server-log", "info", "ZooKeeper server log level")
	pause := fs.Duration("pause", 1500*time.Millisecond, "pause between steps")
	timeout := fs.Duration("timeout", 2*time.Second, "session timeout of application processes")
	fs.Parse(args)
	scenarios := fs.Args()
	if len(scenarios) == 0 || (len(scenarios) == 1 && scenarios[0] == "all") {
		scenarios = app.Scenarios
	}

	sink := zlog.NewTerminalSink()
	net := transport.NewMemNetwork()
	sink.Banner("ZooKeeper demo: starting a 3-server ensemble in this process",
		"Logs: time · level · component │ message key=value",
		"Components: server-N = ZooKeeper servers, other names = application processes,",
		"name/lock, name/election… = recipes running inside a process.",
		"★ marks major transitions. Server logs at "+*serverLevel+", applications at "+*logLevel+".")
	ens, err := app.StartEnsemble(app.EnsembleConfig{
		Peers:     map[int64]string{1: "zk-1", 2: "zk-2", 3: "zk-3"},
		Transport: net,
		Logger:    sink.Logger(level(*serverLevel)),
		Tick:      50 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	defer ens.Close()
	sink.Step("Ensemble ready: server-%d is the leader", ens.Leader())

	d := &app.Demo{
		Env: &app.Env{Servers: ens.Addrs(), Transport: net, Sink: sink, Level: level(*logLevel),
			SessionTimeout: *timeout},
		Ensemble: ens,
		Sink:     sink,
		Pause:    *pause,
	}
	return d.Run(ctx, scenarios)
}

// ---------------------------------------------------------------------------

func runWeb(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("web", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "HTTP listen address")
	n := fs.Int("n", 0, "nodes to start at launch (default 0: add them from the UI)")
	serverLevel := fs.String("server-log", "info", "initial server log level (changeable in the UI)")
	appLevel := fs.String("log", "debug", "initial application log level (changeable in the UI)")
	echo := fs.Bool("echo", false, "also print every log line in this terminal")
	fs.Parse(args)

	opts := web.Options{Size: *n, ServerLevel: level(*serverLevel), AppLevel: level(*appLevel)}
	if *echo {
		opts.Echo = os.Stderr
	}
	lab, err := web.New(opts)
	if err != nil {
		return err
	}
	defer lab.Close()
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: lab.Handler()}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	fmt.Printf("\n  ZooKeeper Lab is running → http://%s\n\n  Open it in your browser. Ctrl+C to stop.\n\n", ln.Addr())
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func runCluster(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cluster", flag.ExitOnError)
	n := fs.Int("n", 3, "number of servers")
	port := fs.Int("port", 2181, "port of the first server (others follow)")
	dir := fs.String("dir", "", "data directory (empty: in memory)")
	logLevel := fs.String("log", "info", "server log level (debug shows every proposal/commit)")
	tick := fs.Duration("tick", 100*time.Millisecond, "tick time")
	fs.Parse(args)

	sink := zlog.NewTerminalSink()
	peers := map[int64]string{}
	var addrs []string
	for i := 1; i <= *n; i++ {
		peers[int64(i)] = fmt.Sprintf("127.0.0.1:%d", *port+i-1)
		addrs = append(addrs, peers[int64(i)])
	}
	sink.Banner(fmt.Sprintf("Starting a %d-server ZooKeeper ensemble over TCP", *n),
		"servers: "+strings.Join(addrs, ", "),
		"Connect applications from other terminals, e.g.:",
		"  zkapp service -name api-1     zkapp gateway     zkapp scheduler -name s1",
		"Ctrl+C stops the ensemble.")
	ens, err := app.StartEnsemble(app.EnsembleConfig{Peers: peers, Transport: transport.TCP{},
		Logger: sink.Logger(level(*logLevel)), Tick: *tick, DataDir: *dir})
	if err != nil {
		return err
	}
	sink.Step("Ensemble ready, leader is server-%d. Waiting for clients…", ens.Leader())
	<-ctx.Done()
	sink.Step("Stopping the ensemble")
	ens.Close()
	return nil
}

// ---------------------------------------------------------------------------

func runApp(ctx context.Context, cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	defServers := defaultServers
	if env := os.Getenv("ZK_SERVERS"); env != "" {
		defServers = env
	}
	servers := fs.String("servers", defServers, "ZooKeeper servers (default from $ZK_SERVERS)")
	defLevel := "debug"
	if cmd == "tree" {
		defLevel = "warn" // keep the tree readable
	}
	logLevel := fs.String("log", defLevel, "log level (trace|debug|info|warn)")
	timeout := fs.Duration("timeout", 4*time.Second, "session timeout")
	name := fs.String("name", cmd, "process name shown in logs")
	service := fs.String("service", "api", "service name")
	interval := fs.Duration("interval", time.Second, "request / job interval")
	count := fs.Int("count", 10, "number of tickets or jobs")
	nolock := fs.Bool("nolock", false, "sell tickets without the lock (to see overselling)")
	store := fs.String("store", filepath.Join(os.TempDir(), "zkapp-tickets.json"), "ticket store file")
	work := fs.Duration("work", 2*time.Second, "time to process a job")
	listen := fs.String("listen", "127.0.0.1:0", "service: HTTP listen address")
	advertise := fs.String("advertise", "", "service: host[:port] registered in ZooKeeper (default: listen address)")
	fs.Parse(args)

	sink := zlog.NewTerminalSink()
	env := &app.Env{Servers: strings.Split(*servers, ","), Transport: transport.TCP{}, Sink: sink,
		Level: level(*logLevel), SessionTimeout: *timeout}

	if cmd == "tickets-init" {
		if err := app.InitTickets(*store, *count); err != nil {
			return err
		}
		sink.Step("ticket store %s reset to %d tickets", *store, *count)
		return nil
	}

	p, err := env.Start(ctx, *name)
	if err != nil {
		return err
	}
	defer p.Stop()

	switch cmd {
	case "service":
		si, err := app.StartServiceInstanceOn(p, *service, *listen, *advertise)
		if err != nil {
			return err
		}
		sink.Step("%s serving on http://%s — registered as %s. Ctrl+C to stop gracefully.", *name, si.Addr, si.Znode)
		<-ctx.Done()
	case "gateway":
		sink.Step("gateway routing a request every %v to the live instances of %q", *interval, *service)
		return app.NewGateway(p, *service).Run(*interval)
	case "config-service":
		return app.NewConfigurableService(p).Run()
	case "config-set":
		updates := app.Settings{}
		for _, kv := range fs.Args() {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return fmt.Errorf("expected key=value, got %q", kv)
			}
			updates[k] = v
		}
		if len(updates) == 0 {
			return fmt.Errorf("usage: zkapp config-set key=value [key=value...]")
		}
		_, err := app.SetConfig(p.Client, updates)
		return err
	case "scheduler":
		return app.NewScheduler(p, *interval).Run()
	case "tickets-agent":
		a := app.NewTicketAgent(p, *store, !*nolock)
		a.ProcessingTime = 200 * time.Millisecond
		err := a.Run()
		if st, e := app.LoadTickets(*store); e == nil {
			sink.Step("%s sold %d tickets; store: remaining=%d, %d sale records", *name, a.Sold, st.Remaining, len(st.Sold))
		}
		return err
	case "queue-produce":
		return app.Produce(p.Client, *count, "job")
	case "queue-worker":
		return app.NewWorker(p, *work).Run()
	case "tree":
		root := "/"
		if fs.NArg() > 0 {
			root = fs.Arg(0)
		}
		return app.PrintTree(p.Client, root, sink.Println)
	}
	return nil
}
