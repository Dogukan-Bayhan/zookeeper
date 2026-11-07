// Package zlog is a human friendly slog handler for watching a ZooKeeper
// ensemble and its clients in a terminal. Every line shows the time, the
// level, the component that produced it (server-1, client api-1, a recipe…)
// and the message followed by key=value attributes:
//
//	15:04:05.123 INFO  server-2          │ follower synchronized, serving clients epoch=2
//	15:04:05.130 DEBUG gateway/discovery │ → request op=getChildren path=/services/api
//
// The special attribute "comp" names the component; nested components are
// joined with "/" (a recipe running in client "gateway" is "gateway/lock").
package zlog

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// LevelTrace is below Debug and shows heartbeats and raw message traffic.
const LevelTrace = slog.Level(-8)

// ParseLevel accepts trace, debug, info, warn and error.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace":
		return LevelTrace, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("unknown log level %q (trace, debug, info, warn, error)", s)
}

// Discard returns a logger that drops everything.
func Discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// OrDiscard returns l, or a discarding logger if l is nil.
func OrDiscard(l *slog.Logger) *slog.Logger {
	if l == nil {
		return Discard()
	}
	return l
}

// New returns a logger writing to w at the given level. Colors are used when
// color is true.
func New(w io.Writer, level slog.Leveler, color bool) *slog.Logger {
	return slog.New(&Handler{out: &output{w: w}, level: level, color: color})
}

// NewTerminal returns a logger for stderr, colored unless NO_COLOR is set or
// stderr is not a terminal.
func NewTerminal(level slog.Leveler) *slog.Logger {
	return NewTerminalSink().Logger(level)
}

// Sink is a shared output: loggers created from the same sink never
// interleave their lines, even with different levels (e.g. servers at INFO
// and clients at DEBUG in the same terminal).
type Sink struct {
	out   *output
	color bool
}

// NewTerminalSink returns a sink writing to stderr.
func NewTerminalSink() *Sink {
	color := os.Getenv("NO_COLOR") == "" &&
		(os.Getenv("FORCE_COLOR") != "" || (isTerminal(os.Stderr) && enableColors(os.Stderr)))
	return &Sink{out: &output{w: os.Stderr}, color: color}
}

// NewSink returns a sink writing to w.
func NewSink(w io.Writer, color bool) *Sink {
	return &Sink{out: &output{w: w}, color: color}
}

// Logger returns a logger writing to the sink at the given level.
func (s *Sink) Logger(level slog.Leveler) *slog.Logger {
	return slog.New(&Handler{out: s.out, level: level, color: s.color})
}

// Color reports whether the sink uses ANSI colors.
func (s *Sink) Color() bool { return s.color }

// Banner prints a framed title with explanation lines, used to separate
// the steps of a demo from the logs.
func (s *Sink) Banner(title string, lines ...string) {
	if s.out.obs != nil {
		s.out.obs.Banner(title, lines)
	}
	bar := strings.Repeat("═", 78)
	var b strings.Builder
	c := func(code, txt string) string {
		if s.color {
			return code + txt + reset
		}
		return txt
	}
	b.WriteString("\n" + c("\x1b[1;36m", bar) + "\n")
	b.WriteString(c("\x1b[1;97m", "  "+title) + "\n")
	if len(lines) > 0 {
		b.WriteString(c("\x1b[36m", strings.Repeat("─", 78)) + "\n")
		for _, l := range lines {
			b.WriteString("  " + l + "\n")
		}
	}
	b.WriteString(c("\x1b[1;36m", bar) + "\n\n")
	s.write(b.String())
}

// Step prints a highlighted narrative line.
func (s *Sink) Step(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if s.out.obs != nil {
		s.out.obs.Step(msg)
	}
	if s.color {
		msg = "\x1b[1;96m▶ " + msg + reset
	} else {
		msg = "▶ " + msg
	}
	s.write("\n" + time.Now().Format("15:04:05.000") + " " + msg + "\n\n")
}

// Result prints a check result line.
func (s *Sink) Result(ok bool, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if s.out.obs != nil {
		s.out.obs.Result(ok, msg)
	}
	mark, code := "✔ ", "\x1b[1;32m"
	if !ok {
		mark, code = "✘ ", "\x1b[1;31m"
	}
	if s.color {
		msg = code + mark + msg + reset
	} else {
		msg = mark + msg
	}
	s.write("   " + msg + "\n")
}

// Println prints a raw line.
func (s *Sink) Println(line string) {
	if s.out.obs != nil {
		s.out.obs.Line(line)
	}
	s.write(line + "\n")
}

func (s *Sink) write(txt string) {
	if s.out.w == nil {
		return
	}
	s.out.mu.Lock()
	defer s.out.mu.Unlock()
	io.WriteString(s.out.w, txt)
}

// Mutable wraps a handler so that it can be silenced at runtime (used to
// simulate a crashed process that stops producing output).
type Mutable struct {
	slog.Handler
	muted *atomicBool
}

type atomicBool struct {
	mu sync.Mutex
	v  bool
}

// NewMutable wraps l; call the returned function to mute it.
func NewMutable(l *slog.Logger) (*slog.Logger, func()) {
	m := &Mutable{Handler: l.Handler(), muted: &atomicBool{}}
	return slog.New(m), func() {
		m.muted.mu.Lock()
		m.muted.v = true
		m.muted.mu.Unlock()
	}
}

func (m *Mutable) Enabled(ctx context.Context, l slog.Level) bool {
	m.muted.mu.Lock()
	muted := m.muted.v
	m.muted.mu.Unlock()
	return !muted && m.Handler.Enabled(ctx, l)
}

func (m *Mutable) WithAttrs(a []slog.Attr) slog.Handler {
	return &Mutable{Handler: m.Handler.WithAttrs(a), muted: m.muted}
}

func (m *Mutable) WithGroup(n string) slog.Handler {
	return &Mutable{Handler: m.Handler.WithGroup(n), muted: m.muted}
}

func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

type output struct {
	mu  sync.Mutex
	w   io.Writer // may be nil when only an observer is used
	obs Observer
}

// KV is a formatted attribute.
type KV struct {
	K string `json:"k"`
	V string `json:"v"`
}

// Record is a structured log line delivered to observers.
type Record struct {
	Time  time.Time
	Level slog.Level
	Comp  string
	Msg   string
	Attrs []KV
}

// LevelName returns TRACE, DEBUG, INFO, WARN or ERROR.
func LevelName(l slog.Level) string {
	n, _ := levelText(l)
	return strings.TrimSpace(n)
}

// Observer receives everything written to a sink in structured form (used
// by the web UI to stream logs to browsers). Implementations must be safe
// for concurrent use and must not block.
type Observer interface {
	Record(Record)
	Banner(title string, lines []string)
	Step(msg string)
	Result(ok bool, msg string)
	Line(text string)
}

// NewObservedSink returns a sink that delivers everything to obs and, if w
// is not nil, also writes it as text to w.
func NewObservedSink(w io.Writer, color bool, obs Observer) *Sink {
	return &Sink{out: &output{w: w, obs: obs}, color: color}
}

func flatten(a slog.Attr, out []KV) []KV {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return out
	}
	if a.Value.Kind() == slog.KindGroup {
		for _, g := range a.Value.Group() {
			g.Key = a.Key + "." + g.Key
			out = flatten(g, out)
		}
		return out
	}
	v := a.Value.String()
	if a.Value.Kind() == slog.KindDuration {
		v = roundDuration(a.Value.Duration()).String()
	}
	return append(out, KV{a.Key, v})
}

// Handler implements slog.Handler.
type Handler struct {
	out   *output
	level slog.Leveler
	color bool
	comp  string
	attrs []slog.Attr
	group string
}

func (h *Handler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level.Level()
}

func (h *Handler) clone() *Handler {
	c := *h
	c.attrs = append([]slog.Attr(nil), h.attrs...)
	return &c
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := h.clone()
	for _, a := range attrs {
		if a.Key == "comp" && h.group == "" {
			if c.comp == "" {
				c.comp = a.Value.String()
			} else {
				c.comp += "/" + a.Value.String()
			}
			continue
		}
		if h.group != "" {
			a.Key = h.group + "." + a.Key
		}
		c.attrs = append(c.attrs, a)
	}
	return c
}

func (h *Handler) WithGroup(name string) slog.Handler {
	c := h.clone()
	if c.group == "" {
		c.group = name
	} else {
		c.group += "." + name
	}
	return c
}

const (
	reset = "\x1b[0m"
	dim   = "\x1b[2m"
	bold  = "\x1b[1m"
)

var compColors = []string{
	"\x1b[36m", "\x1b[35m", "\x1b[34m", "\x1b[32m", "\x1b[33m",
	"\x1b[96m", "\x1b[95m", "\x1b[94m", "\x1b[92m", "\x1b[93m",
}

func levelText(l slog.Level) (string, string) {
	switch {
	case l < slog.LevelDebug:
		return "TRACE", "\x1b[90m"
	case l < slog.LevelInfo:
		return "DEBUG", "\x1b[37m"
	case l < slog.LevelWarn:
		return "INFO ", "\x1b[1;32m"
	case l < slog.LevelError:
		return "WARN ", "\x1b[1;33m"
	}
	return "ERROR", "\x1b[1;31m"
}

func compColor(comp string) string {
	root := comp
	if i := strings.IndexByte(root, '/'); i >= 0 {
		root = root[:i]
	}
	h := fnv.New32a()
	h.Write([]byte(root))
	return compColors[h.Sum32()%uint32(len(compColors))]
}

func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	paint := func(code, s string) {
		if h.color {
			b.WriteString(code)
			b.WriteString(s)
			b.WriteString(reset)
		} else {
			b.WriteString(s)
		}
	}
	paint(dim, r.Time.Format("15:04:05.000"))
	b.WriteByte(' ')
	lt, lc := levelText(r.Level)
	paint(lc, lt)
	b.WriteByte(' ')

	comp := h.comp
	var attrs []slog.Attr
	attrs = append(attrs, h.attrs...)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "comp" && h.group == "" {
			if comp == "" {
				comp = a.Value.String()
			} else {
				comp += "/" + a.Value.String()
			}
			return true
		}
		if h.group != "" {
			a.Key = h.group + "." + a.Key
		}
		attrs = append(attrs, a)
		return true
	})
	if comp == "" {
		comp = "-"
	}
	if obs := h.out.obs; obs != nil {
		var kvs []KV
		for _, a := range attrs {
			kvs = flatten(a, kvs)
		}
		obs.Record(Record{Time: r.Time, Level: r.Level, Comp: comp, Msg: r.Message, Attrs: kvs})
	}
	if h.out.w == nil {
		return nil
	}
	paint(compColor(comp), fmt.Sprintf("%-18s", comp))
	paint(dim, "│ ")

	msg := r.Message
	if r.Level >= slog.LevelWarn || (r.Level >= slog.LevelInfo && strings.HasPrefix(msg, "★")) {
		paint(bold, msg)
	} else {
		b.WriteString(msg)
	}
	for _, a := range attrs {
		writeAttr(&b, a, h.color)
	}
	b.WriteByte('\n')

	h.out.mu.Lock()
	defer h.out.mu.Unlock()
	_, err := io.WriteString(h.out.w, b.String())
	return err
}

func writeAttr(b *strings.Builder, a slog.Attr, color bool) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		for _, g := range a.Value.Group() {
			g.Key = a.Key + "." + g.Key
			writeAttr(b, g, color)
		}
		return
	}
	b.WriteByte(' ')
	if color {
		b.WriteString("\x1b[90m" + a.Key + "=" + reset)
	} else {
		b.WriteString(a.Key + "=")
	}
	var v string
	switch a.Value.Kind() {
	case slog.KindDuration:
		v = roundDuration(a.Value.Duration()).String()
	default:
		v = a.Value.String()
	}
	if v == "" || strings.ContainsAny(v, " \t\n\"=") {
		v = fmt.Sprintf("%q", v)
	}
	b.WriteString(v)
}

func roundDuration(d time.Duration) time.Duration {
	switch {
	case d > time.Second:
		return d.Round(time.Millisecond)
	case d > time.Millisecond:
		return d.Round(10 * time.Microsecond)
	}
	return d
}

// ---- helpers for common attributes ----

// Zxid formats a zxid as epoch:counter, e.g. "2:15".
func Zxid(z int64) slog.Attr {
	return slog.String("zxid", ZxidString(z))
}

// ZxidString formats a zxid as epoch:counter.
func ZxidString(z int64) string {
	return fmt.Sprintf("%d:%d", z>>32, z&0xffffffff)
}

// Session formats a session id in hex.
func Session(id int64) slog.Attr {
	return slog.String("session", SessionString(id))
}

// SessionString formats a session id in hex.
func SessionString(id int64) string {
	return fmt.Sprintf("%x", uint64(id))
}

// Err formats an error attribute.
func Err(err error) slog.Attr {
	if err == nil {
		return slog.String("err", "nil")
	}
	return slog.String("err", err.Error())
}
