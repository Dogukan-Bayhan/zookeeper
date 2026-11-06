// Package transport provides the message-oriented, FIFO connections used
// between ZooKeeper clients and servers and among servers. Like the paper
// (Section 4.2) we rely on the transport for ordered, reliable delivery on a
// connection; a broken connection is reported as an error and never silently
// loses a message in the middle of the stream.
//
// Two implementations are provided: TCP (gob-encoded messages) and an
// in-process network that is convenient for tests and simulations.
package transport

import (
	"errors"
	"sync"
	"time"
)

// ErrClosed is returned by operations on a closed connection or listener.
var ErrClosed = errors.New("transport: closed")

// Conn is a bidirectional, ordered message stream. Send never blocks on the
// peer: messages are queued and written in order by the connection.
type Conn interface {
	Send(msg any) error
	Recv() (any, error)
	Close() error
}

// Listener accepts incoming connections.
type Listener interface {
	Accept() (Conn, error)
	Close() error
	Addr() string
}

// Transport creates listeners and connections.
type Transport interface {
	Listen(addr string) (Listener, error)
	Dial(addr string, timeout time.Duration) (Conn, error)
}

// queue is an unbounded FIFO. Unbounded queues on the send side guarantee that
// two peers sending to each other can never deadlock.
type queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []any
	closed bool
}

func newQueue() *queue {
	q := &queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *queue) push(v any) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.items = append(q.items, v)
	q.cond.Signal()
	return true
}

// pop blocks until an item is available. After close, remaining items are
// still drained before (nil, false) is returned.
func (q *queue) pop() (any, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return nil, false
	}
	v := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	return v, true
}

// close stops accepting new items; queued items can still be popped.
func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

// abort closes the queue and drops queued items.
func (q *queue) abort() {
	q.mu.Lock()
	q.closed = true
	q.items = nil
	q.cond.Broadcast()
	q.mu.Unlock()
}
