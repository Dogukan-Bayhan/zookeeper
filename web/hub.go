package web

import (
	"sync"
	"time"

	"zookeeper/zlog"
)

// Event is one item of the live stream shown in the browser: a log record,
// or a piece of scenario narration (banner, step, check result).
type Event struct {
	Seq   int64     `json:"seq"`
	Kind  string    `json:"kind"` // log | banner | step | result | line
	T     string    `json:"t"`
	Level string    `json:"lvl,omitempty"`
	Comp  string    `json:"comp,omitempty"`
	Msg   string    `json:"msg,omitempty"`
	Attrs []zlog.KV `json:"attrs,omitempty"`
	Lines []string  `json:"lines,omitempty"`
	OK    bool      `json:"ok,omitempty"`
}

// Hub keeps the recent events in a ring buffer and fans them out to the
// connected browsers. It implements zlog.Observer and never blocks loggers:
// a browser that cannot keep up loses events (and is told so).
type Hub struct {
	mu   sync.Mutex
	ring []Event
	next int
	full bool
	seq  int64
	subs map[*subscriber]struct{}
}

type subscriber struct {
	ch      chan Event
	dropped int64
}

// NewHub creates a hub remembering the last size events.
func NewHub(size int) *Hub {
	return &Hub{ring: make([]Event, size), subs: map[*subscriber]struct{}{}}
}

func (h *Hub) publish(ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	ev.Seq = h.seq
	if ev.T == "" {
		ev.T = time.Now().Format("15:04:05.000")
	}
	h.ring[h.next] = ev
	h.next = (h.next + 1) % len(h.ring)
	if h.next == 0 {
		h.full = true
	}
	for s := range h.subs {
		select {
		case s.ch <- ev:
		default:
			s.dropped++
		}
	}
}

// subscribe returns the backlog and a channel of new events.
func (h *Hub) subscribe() (*subscriber, []Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var backlog []Event
	if h.full {
		backlog = append(backlog, h.ring[h.next:]...)
	}
	backlog = append(backlog, h.ring[:h.next]...)
	s := &subscriber{ch: make(chan Event, 20000)}
	h.subs[s] = struct{}{}
	return s, backlog
}

func (h *Hub) unsubscribe(s *subscriber) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

func (h *Hub) takeDropped(s *subscriber) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	d := s.dropped
	s.dropped = 0
	return d
}

// ---- zlog.Observer ----

func (h *Hub) Record(r zlog.Record) {
	h.publish(Event{Kind: "log", T: r.Time.Format("15:04:05.000"), Level: zlog.LevelName(r.Level),
		Comp: r.Comp, Msg: r.Msg, Attrs: r.Attrs})
}

func (h *Hub) Banner(title string, lines []string) {
	h.publish(Event{Kind: "banner", Msg: title, Lines: lines})
}

func (h *Hub) Step(msg string) { h.publish(Event{Kind: "step", Msg: msg}) }

func (h *Hub) Result(ok bool, msg string) { h.publish(Event{Kind: "result", Msg: msg, OK: ok}) }

func (h *Hub) Line(text string) { h.publish(Event{Kind: "line", Msg: text}) }
