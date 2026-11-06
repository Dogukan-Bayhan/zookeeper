package transport

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// MemNetwork is an in-process network. Messages are passed by reference, so
// senders must not modify a message after sending it.
type MemNetwork struct {
	mu        sync.Mutex
	listeners map[string]*memListener
}

// NewMemNetwork creates an empty in-process network.
func NewMemNetwork() *MemNetwork {
	return &MemNetwork{listeners: make(map[string]*memListener)}
}

// Listen registers a listener under addr.
func (n *MemNetwork) Listen(addr string) (Listener, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.listeners[addr]; ok {
		return nil, fmt.Errorf("transport: address %s already in use", addr)
	}
	l := &memListener{net: n, addr: addr, q: newQueue()}
	n.listeners[addr] = l
	return l, nil
}

// Dial connects to the listener registered under addr.
func (n *MemNetwork) Dial(addr string, _ time.Duration) (Conn, error) {
	n.mu.Lock()
	l := n.listeners[addr]
	n.mu.Unlock()
	if l == nil {
		return nil, fmt.Errorf("transport: dial %s: connection refused", addr)
	}
	client := &memConn{in: newQueue()}
	server := &memConn{in: newQueue()}
	client.peer, server.peer = server, client
	if !l.q.push(server) {
		return nil, fmt.Errorf("transport: dial %s: connection refused", addr)
	}
	return client, nil
}

type memListener struct {
	net  *MemNetwork
	addr string
	q    *queue
	once sync.Once
}

func (l *memListener) Accept() (Conn, error) {
	v, ok := l.q.pop()
	if !ok {
		return nil, ErrClosed
	}
	return v.(*memConn), nil
}

func (l *memListener) Close() error {
	l.once.Do(func() {
		l.net.mu.Lock()
		if l.net.listeners[l.addr] == l {
			delete(l.net.listeners, l.addr)
		}
		l.net.mu.Unlock()
		l.q.close()
		for {
			v, ok := l.q.pop()
			if !ok {
				break
			}
			v.(*memConn).Close()
		}
	})
	return nil
}

func (l *memListener) Addr() string { return l.addr }

type memConn struct {
	in     *queue
	peer   *memConn
	closed atomic.Bool
	once   sync.Once
}

func (c *memConn) Send(msg any) error {
	if c.closed.Load() || !c.peer.in.push(msg) {
		return ErrClosed
	}
	return nil
}

func (c *memConn) Recv() (any, error) {
	v, ok := c.in.pop()
	if !ok {
		return nil, io.EOF
	}
	return v, nil
}

func (c *memConn) Close() error {
	c.once.Do(func() {
		c.closed.Store(true)
		c.in.abort()
		// The peer can still read what we sent before closing, like TCP.
		c.peer.in.close()
	})
	return nil
}
