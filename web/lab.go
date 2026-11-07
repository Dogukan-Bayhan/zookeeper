// Package web is an interactive, educational web UI for the ZooKeeper
// implementation. The user builds the ensemble node by node, then picks
// operations from a list (znode operations, applications, failure
// experiments, ready-made scenarios) while watching the live logs and each
// node's replica of the znode tree.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"zookeeper/app"
	"zookeeper/transport"
	"zookeeper/zk"
	"zookeeper/zlog"
)

//go:embed static
var static embed.FS

// Options configures the lab.
type Options struct {
	Size        int // nodes started at launch (0: start empty)
	Tick        time.Duration
	Echo        io.Writer // optional: also print the logs as text
	ServerLevel slog.Level
	AppLevel    slog.Level
}

// Lab owns the ensemble and the application processes.
type Lab struct {
	hub         *Hub
	sink        *zlog.Sink
	serverLevel *slog.LevelVar
	appLevel    *slog.LevelVar
	tick        time.Duration
	nodeOps     sync.Mutex // serializes node membership operations

	mu       sync.Mutex
	net      *transport.MemNetwork
	ens      *app.Ensemble
	env      *app.Env
	console  *app.Process
	procs    []*app.Process
	counters map[string]int
	pause    time.Duration
	busy     string // node operation in progress

	scenario     string
	scenarioDemo *app.Demo
	scenarioStop context.CancelFunc
	scenarioDone chan struct{}
	lastResult   string
}

// Errors shown to the user.
var (
	errNoNodes  = errors.New("önce en az bir node ekle")
	errNoQuorum = errors.New("cluster hizmet vermiyor: çoğunluk yok ya da lider seçiliyor")
)

// New creates a lab.
func New(o Options) (*Lab, error) {
	if o.Tick == 0 {
		o.Tick = 50 * time.Millisecond
	}
	l := &Lab{
		hub:         NewHub(30000),
		serverLevel: new(slog.LevelVar),
		appLevel:    new(slog.LevelVar),
		tick:        o.Tick,
		counters:    map[string]int{},
		pause:       2 * time.Second,
	}
	l.serverLevel.Set(o.ServerLevel)
	l.appLevel.Set(o.AppLevel)
	color := false
	if f, ok := o.Echo.(*os.File); ok && f == os.Stderr {
		color = zlog.NewTerminalSink().Color()
	}
	l.sink = zlog.NewObservedSink(o.Echo, color, l.hub)
	l.resetEmpty()
	if o.Size > 0 {
		if err := l.grow(o.Size); err != nil {
			return nil, err
		}
	}
	return l, nil
}

// resetEmpty stops everything and creates an empty ensemble.
func (l *Lab) resetEmpty() {
	l.stopScenario()
	l.mu.Lock()
	procs := append(l.procs, l.console)
	ens := l.ens
	l.procs, l.console = nil, nil
	l.counters = map[string]int{}
	l.lastResult = ""
	l.net = transport.NewMemNetwork()
	l.ens = app.NewEnsemble(app.EnsembleConfig{Transport: l.net, Logger: l.sink.Logger(l.serverLevel), Tick: l.tick})
	l.mu.Unlock()
	for _, p := range procs {
		if p != nil {
			p.Stop()
		}
	}
	if ens != nil {
		ens.Close()
	}
	l.rebuildEnv()
}

// rebuildEnv points new processes at the current member list. The console
// client is recreated lazily so that it knows every node.
func (l *Lab) rebuildEnv() {
	l.mu.Lock()
	l.env = &app.Env{Servers: l.ens.Addrs(), Transport: l.net, Sink: l.sink, Level: l.appLevel,
		SessionTimeout: 3 * time.Second}
	console := l.console
	l.console = nil
	l.mu.Unlock()
	if console != nil {
		go console.Stop()
	}
}

func (l *Lab) grow(n int) error {
	l.nodeOps.Lock()
	defer l.nodeOps.Unlock()
	ens := l.ensemble()
	before := ens.Size()
	if before+n > 7 {
		return errors.New("en fazla 7 node")
	}
	l.setBusy(fmt.Sprintf("%d node ekleniyor…", n))
	defer l.setBusy("")
	if before == 0 {
		l.sink.Banner(fmt.Sprintf("Starting a %d-node ensemble", n),
			"Nodes elect a leader (votes ordered by epoch, zxid, id), then run Zab:",
			"discovery (new epoch) → synchronization (DIFF/SNAP) → broadcast.")
	} else {
		l.sink.Step("Adding %d node(s): membership is static, so the %d running node(s) restart with the new configuration (rolling restart). Their data survives on disk.", n, before)
	}
	_, err := ens.Grow(n)
	l.rebuildEnv()
	return err
}

func (l *Lab) setBusy(s string) {
	l.mu.Lock()
	l.busy = s
	l.mu.Unlock()
}

func (l *Lab) ensemble() *app.Ensemble {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ens
}

// ready reports whether the cluster can serve clients.
func (l *Lab) ready() error {
	ens := l.ensemble()
	if ens.Size() == 0 {
		return errNoNodes
	}
	if ens.Leader() == 0 {
		return errNoQuorum
	}
	return nil
}

// consoleClient returns the client used for manual operations.
func (l *Lab) consoleClient() (*app.Process, error) {
	if err := l.ready(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	c, env := l.console, l.env
	l.mu.Unlock()
	if c != nil && c.Status() == "running" {
		return c, nil
	}
	c, err := env.Start(context.Background(), "console")
	if err != nil {
		return nil, err
	}
	c.Kind = "console"
	l.mu.Lock()
	if l.console != nil && l.console.Status() == "running" {
		other := l.console
		l.mu.Unlock()
		c.Stop()
		return other, nil
	}
	l.console = c
	l.mu.Unlock()
	return c, nil
}

// Handler returns the HTTP handler of the lab.
func (l *Lab) Handler() http.Handler {
	mux := http.NewServeMux()
	sub, _ := fs.Sub(static, "static")
	mux.Handle("GET /", http.FileServerFS(sub))
	mux.HandleFunc("GET /api/events", l.handleEvents)
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, l.state()) })
	mux.HandleFunc("GET /api/tree", l.handleTree)
	mux.HandleFunc("POST /api/nodes", l.handleNodes)
	mux.HandleFunc("POST /api/scenario", l.handleScenario)
	mux.HandleFunc("POST /api/spawn", l.handleSpawn)
	mux.HandleFunc("POST /api/process", l.handleProcess)
	mux.HandleFunc("POST /api/action", l.handleAction)
	mux.HandleFunc("POST /api/znode", l.handleZnode)
	mux.HandleFunc("POST /api/levels", l.handleLevels)
	return mux
}

// Close stops everything.
func (l *Lab) Close() {
	l.stopScenario()
	l.mu.Lock()
	procs := append(l.procs, l.console)
	ens := l.ens
	l.mu.Unlock()
	for _, p := range procs {
		if p != nil {
			p.Stop()
		}
	}
	ens.Close()
}

// ---------------------------------------------------------------------------
// helpers

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(v)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

type result struct {
	OK     bool   `json:"ok"`
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func reply(w http.ResponseWriter, res any, err error) {
	if err != nil {
		writeJSON(w, result{Error: err.Error()})
		return
	}
	writeJSON(w, result{OK: true, Result: res})
}

// ---------------------------------------------------------------------------
// live events (Server-Sent Events)

func (l *Lab) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	sub, backlog := l.hub.subscribe()
	defer l.hub.unsubscribe(sub)
	send := func(event string, v any) bool {
		data, _ := json.Marshal(v)
		_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		return err == nil
	}
	if !send("logs", backlog) || !send("state", l.state()) {
		return
	}
	flusher.Flush()

	batch := time.NewTicker(100 * time.Millisecond)
	defer batch.Stop()
	stateTick := time.NewTicker(400 * time.Millisecond)
	defer stateTick.Stop()
	var pending []Event
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-sub.ch:
			pending = append(pending, ev)
			if len(pending) < 2000 {
				continue
			}
		case <-batch.C:
		case <-stateTick.C:
			if !send("state", l.state()) {
				return
			}
			flusher.Flush()
			continue
		}
		if d := l.hub.takeDropped(sub); d > 0 {
			pending = append(pending, Event{Kind: "line", Msg: fmt.Sprintf("… %d log lines dropped (browser too slow)", d)})
		}
		if len(pending) > 0 {
			if !send("logs", pending) {
				return
			}
			pending = pending[:0]
			flusher.Flush()
		}
	}
}

// ---------------------------------------------------------------------------
// state

type nodeView struct {
	ID          int64  `json:"id"`
	Running     bool   `json:"running"`
	State       string `json:"state"`
	Serving     bool   `json:"serving"`
	Epoch       int64  `json:"epoch"`
	LastApplied string `json:"lastApplied"`
	Znodes      int    `json:"znodes"`
	Sessions    int    `json:"sessions"`
	Clients     int    `json:"clients"`
	Followers   int    `json:"followers"`
	Digest      string `json:"digest"`
}

type procView struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Status  string `json:"status"`
	Session string `json:"session"`
	Node    string `json:"node"`
	Note    string `json:"note"`
	Source  string `json:"source"`
}

type stateView struct {
	Size      int        `json:"size"`
	Running   int        `json:"running"`
	Quorum    int        `json:"quorum"`
	Leader    int64      `json:"leader"`
	Busy      string     `json:"busy"`
	Nodes     []nodeView `json:"nodes"`
	Processes []procView `json:"processes"`
	Scenario  string     `json:"scenario"`
	Result    string     `json:"result"`
	Levels    struct {
		Server string `json:"server"`
		App    string `json:"app"`
	} `json:"levels"`
}

func (l *Lab) state() stateView {
	var st stateView
	l.mu.Lock()
	ens := l.ens
	st.Busy = l.busy
	st.Scenario = l.scenario
	st.Result = l.lastResult
	procs := append([]*app.Process(nil), l.procs...)
	demo := l.scenarioDemo
	l.mu.Unlock()
	st.Levels.Server = strings.ToLower(zlog.LevelName(l.serverLevel.Level()))
	st.Levels.App = strings.ToLower(zlog.LevelName(l.appLevel.Level()))

	ids := ens.IDs()
	st.Size = len(ids)
	st.Quorum = st.Size/2 + 1
	st.Leader = ens.Leader()
	st.Nodes = []nodeView{}
	for _, id := range ids {
		v := nodeView{ID: id, State: "STOPPED"}
		if s := ens.Server(id); s != nil {
			x := s.Stats()
			st.Running++
			v.Running, v.State, v.Serving = true, x.State.String(), x.Serving
			v.Epoch = x.CurrentEpoch
			v.LastApplied = zlog.ZxidString(x.LastApplied)
			v.Znodes, v.Sessions, v.Clients, v.Followers = x.Znodes, x.Sessions, x.LocalClients, x.Followers
			v.Digest = fmt.Sprintf("%x", x.Digest[:4])
		}
		st.Nodes = append(st.Nodes, v)
	}
	view := func(p *app.Process, source string) procView {
		v := procView{Name: p.Name, Kind: p.Kind, Status: p.Status(), Note: p.Note, Source: source}
		if v.Kind == "" {
			v.Kind = kindOf(p.Name)
		}
		if v.Status == "running" {
			v.Session = zlog.SessionString(p.Client.SessionID())
			v.Node = strings.Replace(p.Client.Server(), "zk-", "node ", 1)
			if p.Client.State() != zk.StateConnected {
				v.Status = "reconnecting"
			}
		}
		return v
	}
	st.Processes = []procView{}
	for _, p := range procs {
		st.Processes = append(st.Processes, view(p, "playground"))
	}
	if demo != nil {
		for _, p := range demo.Processes() {
			if p.Name != "inspector" {
				st.Processes = append(st.Processes, view(p, "scenario"))
			}
		}
	}
	return st
}

var kindByPrefix = []struct{ prefix, kind string }{
	{"api", "service"}, {"gateway", "gateway"}, {"scheduler", "scheduler"}, {"shop", "config-service"},
	{"worker", "worker"}, {"agent", "ticket-agent"}, {"producer", "producer"}, {"operator", "operator"},
}

func kindOf(name string) string {
	for _, k := range kindByPrefix {
		if strings.HasPrefix(name, k.prefix) {
			return k.kind
		}
	}
	return "process"
}

// ---------------------------------------------------------------------------
// nodes

func (l *Lab) handleNodes(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Action string `json:"action"`
		ID     int64  `json:"id"`
		Count  int    `json:"count"`
	}
	if err := readJSON(r, &req); err != nil {
		reply(w, nil, err)
		return
	}
	ens := l.ensemble()
	known := ens.Addr(req.ID) != ""
	switch req.Action {
	case "add":
		n := max(1, req.Count)
		reply(w, "ok", l.grow(n))
	case "remove":
		if !known {
			reply(w, nil, fmt.Errorf("node %d yok", req.ID))
			return
		}
		l.nodeOps.Lock()
		defer l.nodeOps.Unlock()
		l.setBusy(fmt.Sprintf("node %d kaldırılıyor…", req.ID))
		defer l.setBusy("")
		l.sink.Step("Removing node %d: the remaining running nodes restart with the smaller configuration", req.ID)
		err := ens.RemoveServer(req.ID)
		l.rebuildEnv()
		reply(w, "ok", err)
	case "stop":
		if ens.Server(req.ID) == nil {
			reply(w, nil, errors.New("node zaten durmuş"))
			return
		}
		role := ens.Server(req.ID).State()
		l.sink.Step("💥 Node %d crashed (it was %v). Its storage survives; watch the others react.", req.ID, role)
		ens.StopServer(req.ID)
		reply(w, "ok", nil)
	case "start":
		if !known {
			reply(w, nil, fmt.Errorf("node %d yok", req.ID))
			return
		}
		if ens.Server(req.ID) != nil {
			reply(w, nil, errors.New("node zaten çalışıyor"))
			return
		}
		l.sink.Step("Starting node %d: it recovers from its log/snapshot, joins the ensemble and catches up", req.ID)
		reply(w, "ok", ens.StartServer(req.ID))
	case "crash-leader":
		leader := ens.Leader()
		if leader == 0 {
			reply(w, nil, errors.New("şu an lider yok"))
			return
		}
		l.sink.Step("💥 Crashing the LEADER (node %d): followers will notice, elect a new leader and bump the epoch", leader)
		ens.StopServer(leader)
		reply(w, fmt.Sprintf("node %d çökertildi", leader), nil)
	case "reset":
		l.nodeOps.Lock()
		defer l.nodeOps.Unlock()
		l.resetEmpty()
		l.sink.Banner("Everything was reset: no nodes. Add a node to begin.")
		reply(w, "ok", nil)
	default:
		reply(w, nil, fmt.Errorf("bilinmeyen işlem %q", req.Action))
	}
}

// ---------------------------------------------------------------------------
// tree

type znodeView struct {
	Path     string `json:"path"`
	Data     string `json:"data"`
	Version  int32  `json:"version"`
	Cversion int32  `json:"cversion"`
	Owner    string `json:"owner,omitempty"`
	Czxid    string `json:"czxid"`
	Mzxid    string `json:"mzxid"`
	Children int    `json:"children"`
	Bytes    int    `json:"bytes"`
}

func (l *Lab) handleTree(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.URL.Query().Get("node"), 10, 64)
	s := l.ensemble().Server(id)
	if s == nil {
		reply(w, nil, fmt.Errorf("node %d çalışmıyor", id))
		return
	}
	snap := s.Snapshot()
	counts := map[string]int{}
	for _, n := range snap.Nodes {
		if n.Path != "/" {
			counts[zk.Parent(n.Path)]++
		}
	}
	nodes := []znodeView{}
	for _, n := range snap.Nodes {
		data := string(n.Data)
		if len(data) > 4096 {
			data = data[:4096] + "…"
		}
		v := znodeView{Path: n.Path, Data: data, Version: n.Stat.Version, Cversion: n.Stat.Cversion,
			Czxid: zlog.ZxidString(n.Stat.Czxid), Mzxid: zlog.ZxidString(n.Stat.Mzxid),
			Children: counts[n.Path], Bytes: len(n.Data)}
		if n.Stat.EphemeralOwner != 0 {
			v.Owner = zlog.SessionString(n.Stat.EphemeralOwner)
		}
		nodes = append(nodes, v)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Path < nodes[j].Path })
	reply(w, map[string]any{"node": id, "zxid": zlog.ZxidString(snap.Zxid), "nodes": nodes}, nil)
}

// ---------------------------------------------------------------------------
// scenarios

func (l *Lab) handleScenario(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Action  string `json:"action"`
		PauseMs int64  `json:"pauseMs"`
	}
	if err := readJSON(r, &req); err != nil {
		reply(w, nil, err)
		return
	}
	if req.Action == "stop" {
		l.sink.Step("Scenario stopped by the user")
		l.stopScenario()
		reply(w, "stopped", nil)
		return
	}
	names := []string{req.Name}
	if req.Name == "all" {
		names = app.Scenarios
	} else if !slices.Contains(app.Scenarios, req.Name) {
		reply(w, nil, fmt.Errorf("bilinmeyen senaryo %q", req.Name))
		return
	}
	if err := l.ready(); err != nil {
		reply(w, nil, err)
		return
	}
	ens := l.ensemble()
	if slices.Contains(names, "failover") && ens.Size() < 3 {
		reply(w, nil, errors.New("bu senaryo için en az 3 node gerekli (lider ölünce çoğunluk kalmalı)"))
		return
	}
	l.stopScenario()
	l.stopPlayground("a scenario is starting")
	pause := time.Duration(req.PauseMs) * time.Millisecond
	if pause <= 0 {
		pause = l.pause
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.mu.Lock()
	d := &app.Demo{Env: l.env, Ensemble: ens, Sink: l.sink, Pause: pause}
	done := make(chan struct{})
	l.pause = pause
	l.scenario = req.Name
	l.scenarioDemo = d
	l.scenarioStop = cancel
	l.scenarioDone = done
	l.lastResult = ""
	l.mu.Unlock()
	go func() {
		defer close(done)
		err := d.Run(ctx, names)
		res := "başarılı"
		switch {
		case err != nil:
			res = "hata: " + err.Error()
		case ctx.Err() != nil:
			res = "durduruldu"
		case d.Failures() > 0:
			res = fmt.Sprintf("%d kontrol başarısız", d.Failures())
		}
		l.mu.Lock()
		if l.scenarioDemo == d {
			l.scenario = ""
			l.scenarioDemo = nil
			l.lastResult = req.Name + ": " + res
		}
		l.mu.Unlock()
	}()
	reply(w, "started", nil)
}

func (l *Lab) stopScenario() {
	l.mu.Lock()
	cancel, done := l.scenarioStop, l.scenarioDone
	l.scenarioStop, l.scenarioDone = nil, nil
	l.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
	}
	l.mu.Lock()
	l.scenario = ""
	l.scenarioDemo = nil
	l.mu.Unlock()
}

// ---------------------------------------------------------------------------
// applications

func (l *Lab) stopPlayground(reason string) {
	l.mu.Lock()
	procs := l.procs
	l.procs = nil
	l.mu.Unlock()
	running := 0
	for _, p := range procs {
		if p.Status() == "running" {
			running++
		}
		p.Stop()
	}
	if running > 0 {
		l.sink.Step("Stopping %d process(es): %s", running, reason)
	}
}

var spawnPrefix = map[string]string{
	"service": "api", "gateway": "gateway", "scheduler": "scheduler",
	"config-service": "shop", "worker": "worker", "ticket-agent": "agent",
}

func (l *Lab) handleSpawn(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Kind string `json:"kind"`
	}
	if err := readJSON(r, &req); err != nil {
		reply(w, nil, err)
		return
	}
	prefix, ok := spawnPrefix[req.Kind]
	if !ok {
		reply(w, nil, fmt.Errorf("bilinmeyen süreç türü %q", req.Kind))
		return
	}
	if err := l.ready(); err != nil {
		reply(w, nil, err)
		return
	}
	l.mu.Lock()
	if l.scenario != "" {
		l.mu.Unlock()
		reply(w, nil, errors.New("bir senaryo çalışıyor: önce onu durdur"))
		return
	}
	l.counters[prefix]++
	name := fmt.Sprintf("%s-%d", prefix, l.counters[prefix])
	env := l.env
	l.mu.Unlock()

	p, err := env.Start(context.Background(), name)
	if err != nil {
		reply(w, nil, err)
		return
	}
	p.Kind = req.Kind
	switch req.Kind {
	case "service":
		si, err := app.StartServiceInstance(p, "api")
		if err != nil {
			p.Stop()
			reply(w, nil, err)
			return
		}
		p.Note = "http://" + si.Addr
	case "gateway":
		go app.NewGateway(p, "api").Run(time.Second)
		p.Note = "/services/api"
	case "scheduler":
		go app.NewScheduler(p, 2*time.Second).Run()
		p.Note = app.ElectionPath
	case "config-service":
		go app.NewConfigurableService(p).Run()
		p.Note = app.ConfigPath
	case "worker":
		go app.NewWorker(p, 2*time.Second).Run()
		p.Note = app.TasksPath
	case "ticket-agent":
		a := app.NewTicketAgent(p, ticketStore(), true)
		a.ProcessingTime = 300 * time.Millisecond
		go a.Run()
		p.Note = app.LockPath
	}
	l.mu.Lock()
	kept := l.procs[:0]
	for _, q := range l.procs {
		if q.Status() == "running" || time.Since(q.Started) < 5*time.Minute {
			kept = append(kept, q)
		}
	}
	l.procs = append(kept, p)
	l.mu.Unlock()
	reply(w, name, nil)
}

func ticketStore() string { return os.TempDir() + string(os.PathSeparator) + "zklab-tickets.json" }

func (l *Lab) findProcess(name string) *app.Process {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, p := range l.procs {
		if p.Name == name && p.Status() == "running" {
			return p
		}
	}
	if l.scenarioDemo != nil {
		for _, p := range l.scenarioDemo.Processes() {
			if p.Name == name && p.Status() == "running" {
				return p
			}
		}
	}
	return nil
}

func (l *Lab) handleProcess(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string `json:"name"`
		Action string `json:"action"`
	}
	if err := readJSON(r, &req); err != nil {
		reply(w, nil, err)
		return
	}
	if req.Action == "clear" {
		l.mu.Lock()
		kept := l.procs[:0]
		for _, p := range l.procs {
			if p.Status() == "running" {
				kept = append(kept, p)
			}
		}
		l.procs = kept
		l.mu.Unlock()
		reply(w, "ok", nil)
		return
	}
	p := l.findProcess(req.Name)
	if p == nil {
		reply(w, nil, fmt.Errorf("çalışan %q süreci yok", req.Name))
		return
	}
	switch req.Action {
	case "stop":
		l.sink.Step("%s stopped gracefully: it closes its session, its ephemeral znodes vanish at once", req.Name)
		go p.Stop()
	case "crash":
		l.sink.Step("💥 %s crashed: no goodbye to ZooKeeper — its ephemeral znodes vanish only after the session timeout (%v)",
			req.Name, p.Client.SessionTimeout())
		p.Crash()
	default:
		reply(w, nil, fmt.Errorf("bilinmeyen işlem %q", req.Action))
		return
	}
	reply(w, "ok", nil)
}

func (l *Lab) handleAction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Kind  string `json:"kind"`
		Key   string `json:"key"`
		Value string `json:"value"`
		Count int    `json:"count"`
	}
	if err := readJSON(r, &req); err != nil {
		reply(w, nil, err)
		return
	}
	if req.Kind == "tickets-init" {
		n := max(1, min(req.Count, 1000))
		reply(w, fmt.Sprintf("stok %d bilete ayarlandı", n), app.InitTickets(ticketStore(), n))
		return
	}
	if req.Kind == "tickets-status" {
		st, err := app.LoadTickets(ticketStore())
		if err != nil {
			reply(w, nil, errors.New("stok yok: önce stoğu sıfırla"))
			return
		}
		reply(w, fmt.Sprintf("kalan %d · satılan %d", st.Remaining, len(st.Sold)), nil)
		return
	}
	console, err := l.consoleClient()
	if err != nil {
		reply(w, nil, err)
		return
	}
	switch req.Kind {
	case "config-set":
		if req.Key == "" {
			reply(w, nil, errors.New("anahtar gerekli"))
			return
		}
		v, err := app.SetConfig(console.Client, app.Settings{req.Key: req.Value})
		reply(w, fmt.Sprintf("config sürümü %d", v), err)
	case "produce":
		n := max(1, min(req.Count, 100))
		reply(w, fmt.Sprintf("%d iş eklendi", n), app.Produce(console.Client, n, "job"))
	default:
		reply(w, nil, fmt.Errorf("bilinmeyen işlem %q", req.Kind))
	}
}

// ---------------------------------------------------------------------------
// znode operations

func (l *Lab) handleZnode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Op         string `json:"op"`
		Path       string `json:"path"`
		Data       string `json:"data"`
		Ephemeral  bool   `json:"ephemeral"`
		Sequential bool   `json:"sequential"`
		Version    *int32 `json:"version"`
	}
	if err := readJSON(r, &req); err != nil {
		reply(w, nil, err)
		return
	}
	console, err := l.consoleClient()
	if err != nil {
		reply(w, nil, err)
		return
	}
	c, lg := console.Client, console.Log
	version := zk.AnyVersion
	if req.Version != nil {
		version = *req.Version
	}
	switch req.Op {
	case "create":
		var flags zk.CreateFlag
		if req.Ephemeral {
			flags |= zk.FlagEphemeral
		}
		if req.Sequential {
			flags |= zk.FlagSequential
		}
		p, err := c.Create(req.Path, []byte(req.Data), flags)
		reply(w, "oluşturuldu: "+p, err)
	case "set":
		st, err := c.SetData(req.Path, []byte(req.Data), version)
		if err != nil {
			reply(w, nil, err)
			return
		}
		reply(w, fmt.Sprintf("güncellendi, yeni sürüm %d", st.Version), nil)
	case "delete":
		reply(w, "silindi", c.Delete(req.Path, version))
	case "get":
		if err := c.Sync(req.Path); err != nil {
			reply(w, nil, err)
			return
		}
		data, st, err := c.GetData(req.Path, false)
		if err != nil {
			reply(w, nil, err)
			return
		}
		reply(w, fmt.Sprintf("%q · sürüm %d", data, st.Version), nil)
	case "ls":
		if err := c.Sync(req.Path); err != nil {
			reply(w, nil, err)
			return
		}
		kids, err := c.GetChildren(req.Path, false)
		if err != nil {
			reply(w, nil, err)
			return
		}
		if len(kids) == 0 {
			reply(w, "çocuk yok", nil)
			return
		}
		reply(w, strings.Join(kids, ", "), nil)
	case "watch":
		st, ch, err := c.ExistsW(req.Path)
		if err != nil {
			reply(w, nil, err)
			return
		}
		lg.Info("👁 watch set: you will see an event when this znode is created, changed or deleted",
			"path", req.Path, "existsNow", st != nil)
		go func() {
			if ev, ok := <-ch; ok {
				lg.Info("👁 WATCH FIRED (one-time: set it again to keep watching)", "event", ev.Type.String(), "path", ev.Path)
			}
		}()
		reply(w, "watch kuruldu — değişince loglarda görünecek", nil)
	default:
		reply(w, nil, fmt.Errorf("bilinmeyen işlem %q", req.Op))
	}
}

// ---------------------------------------------------------------------------

func (l *Lab) handleLevels(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Server string `json:"server"`
		App    string `json:"app"`
	}
	if err := readJSON(r, &req); err != nil {
		reply(w, nil, err)
		return
	}
	for _, x := range []struct {
		v   string
		dst *slog.LevelVar
	}{{req.Server, l.serverLevel}, {req.App, l.appLevel}} {
		if x.v == "" {
			continue
		}
		lv, err := zlog.ParseLevel(x.v)
		if err != nil {
			reply(w, nil, err)
			return
		}
		x.dst.Set(lv)
	}
	reply(w, "ok", nil)
}
