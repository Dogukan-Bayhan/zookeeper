package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"zookeeper/recipes"
)

// Scenario 2: service discovery and load balancing.
//
// Problem: service instances come and go (deploys, autoscaling, crashes) and
// their addresses are not known in advance. Clients must always know the
// set of live instances, without a hard-coded list and without sending
// traffic to dead machines.
//
// Solution: every instance registers itself with an EPHEMERAL|SEQUENTIAL
// znode under /services/<name> holding its address (Section 2.4, "Group
// Membership"). The znode lives exactly as long as the instance's session:
// a graceful shutdown removes it at once, a crash removes it when the
// session times out. A gateway lists the children with a watch and keeps an
// up-to-date routing table; it never polls.

// ServicesRoot is the parent of all service registries.
const ServicesRoot = "/services"

// Instance is the registration record of a service instance.
type Instance struct {
	Name    string    `json:"name"`
	Addr    string    `json:"addr"`
	Started time.Time `json:"started"`
	znode   string
}

// ServiceInstance is a real HTTP server registered in ZooKeeper.
type ServiceInstance struct {
	p       *Process
	service string
	srv     *http.Server
	Addr    string
	Znode   string
	served  atomic.Int64
}

// StartServiceInstance starts an HTTP server on a random local port and
// registers it.
func StartServiceInstance(p *Process, service string) (*ServiceInstance, error) {
	return StartServiceInstanceOn(p, service, "127.0.0.1:0", "")
}

// StartServiceInstanceOn starts an HTTP server on listen and registers it
// under the address advertise (host[:port]); an empty advertise uses the
// listening address. In containers, listen on 0.0.0.0 and advertise the
// container's host name so other containers can reach the instance.
func StartServiceInstanceOn(p *Process, service, listen, advertise string) (*ServiceInstance, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	addr := ln.Addr().String()
	if advertise != "" {
		if _, _, err := net.SplitHostPort(advertise); err != nil {
			_, port, _ := net.SplitHostPort(addr)
			advertise = net.JoinHostPort(advertise, port)
		}
		addr = advertise
	}
	si := &ServiceInstance{p: p, service: service, Addr: addr}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		n := si.served.Add(1)
		p.Log.Debug("HTTP request served", "path", r.URL.Path, "total", n)
		fmt.Fprintf(w, "hello from %s (request #%d)", p.Name, n)
	})
	si.srv = &http.Server{Handler: mux}
	go si.srv.Serve(ln)
	p.Log.Info("HTTP server listening", "addr", si.Addr)

	dir := ServicesRoot + "/" + service
	if err := recipes.EnsurePath(p.Client, dir); err != nil {
		return nil, err
	}
	body, _ := json.Marshal(Instance{Name: p.Name, Addr: si.Addr, Started: time.Now()})
	si.Znode, err = recipes.JoinGroup(p.Client, dir, "", body)
	if err != nil {
		return nil, err
	}
	p.Log.Info("★ registered in service registry", "service", service, "znode", si.Znode, "addr", si.Addr)
	go func() {
		<-p.Ctx.Done()
		si.srv.Close()
	}()
	return si, nil
}

// Served returns the number of requests served.
func (si *ServiceInstance) Served() int64 { return si.served.Load() }

// Gateway routes requests round-robin over the live instances of a service.
type Gateway struct {
	p       *Process
	service string
	http    *http.Client

	mu        sync.Mutex
	instances []Instance
	next      int
	ok, fail  atomic.Int64
}

// NewGateway creates a gateway for service.
func NewGateway(p *Process, service string) *Gateway {
	return &Gateway{p: p, service: service, http: &http.Client{Timeout: time.Second}}
}

// Instances returns the current routing table.
func (g *Gateway) Instances() []Instance {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]Instance(nil), g.instances...)
}

// Stats returns the number of successful and failed requests.
func (g *Gateway) Stats() (ok, fail int64) { return g.ok.Load(), g.fail.Load() }

// Discover keeps the routing table in sync with the registry.
func (g *Gateway) Discover() error {
	dir := ServicesRoot + "/" + g.service
	c := g.p.Client
	if err := recipes.EnsurePath(c, dir); err != nil {
		return err
	}
	g.p.Log.Info("watching service registry", "path", dir)
	err := recipes.WatchMembers(g.p.Ctx, c, dir, func(members []string) {
		var table []Instance
		for _, m := range members {
			data, _, err := c.GetData(dir+"/"+m, false)
			if err != nil {
				continue // vanished meanwhile
			}
			var in Instance
			if json.Unmarshal(data, &in) == nil {
				in.znode = m
				table = append(table, in)
			}
		}
		sort.Slice(table, func(i, j int) bool { return table[i].znode < table[j].znode })
		g.mu.Lock()
		old := g.instances
		g.instances = table
		g.mu.Unlock()
		added, removed := diffInstances(old, table)
		for _, in := range added {
			g.p.Log.Info("➕ instance discovered, adding to routing table", "instance", in.Name, "addr", in.Addr)
		}
		for _, in := range removed {
			g.p.Log.Warn("➖ instance gone from registry, removing from routing table", "instance", in.Name, "addr", in.Addr)
		}
		g.p.Log.Info("routing table", "service", g.service, "instances", fmt.Sprint(names(table)))
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Route sends one request, trying other instances if the chosen one fails.
func (g *Gateway) Route(n int) {
	tried := map[string]bool{}
	for {
		g.mu.Lock()
		if len(g.instances) == 0 {
			g.mu.Unlock()
			g.fail.Add(1)
			g.p.Log.Warn("no live instance to route to", "request", n)
			return
		}
		in := g.instances[g.next%len(g.instances)]
		g.next++
		total := len(g.instances)
		g.mu.Unlock()
		if tried[in.Addr] {
			g.fail.Add(1)
			g.p.Log.Error("every instance failed", "request", n)
			return
		}
		tried[in.Addr] = true
		resp, err := g.http.Get("http://" + in.Addr + "/api/products")
		if err != nil {
			g.p.Log.Warn("request failed (instance probably crashed; ZooKeeper will remove it when its session expires), trying next",
				"request", n, "instance", in.Name, "err", err.Error())
			if len(tried) >= total {
				g.fail.Add(1)
				return
			}
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		g.ok.Add(1)
		g.p.Log.Info("routed request", "request", n, "to", in.Name, "response", string(body))
		return
	}
}

// Run discovers instances and routes a request every interval.
func (g *Gateway) Run(interval time.Duration) error {
	errc := make(chan error, 1)
	go func() { errc <- g.Discover() }()
	for n := 1; sleepCtx(g.p.Ctx, interval); n++ {
		g.Route(n)
	}
	return <-errc
}

func diffInstances(old, cur []Instance) (added, removed []Instance) {
	in := func(list []Instance, x Instance) bool {
		for _, y := range list {
			if y.znode == x.znode {
				return true
			}
		}
		return false
	}
	for _, x := range cur {
		if !in(old, x) {
			added = append(added, x)
		}
	}
	for _, x := range old {
		if !in(cur, x) {
			removed = append(removed, x)
		}
	}
	return
}

func names(list []Instance) []string {
	out := make([]string, len(list))
	for i, in := range list {
		out[i] = in.Name
	}
	return out
}
