package transport

import (
	"bufio"
	"encoding/gob"
	"net"
	"sync"
	"time"
)

// TCP is a Transport over TCP. Messages are gob encoded; every concrete
// message type must be registered with gob.Register (the zk and server
// packages register theirs).
type TCP struct{}

type envelope struct {
	Msg any
}

// Listen starts a TCP listener on addr.
func (TCP) Listen(addr string) (Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &tcpListener{l: l}, nil
}

// Dial opens a TCP connection to addr.
func (TCP) Dial(addr string, timeout time.Duration) (Conn, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return newTCPConn(c), nil
}

type tcpListener struct{ l net.Listener }

func (l *tcpListener) Accept() (Conn, error) {
	c, err := l.l.Accept()
	if err != nil {
		return nil, err
	}
	return newTCPConn(c), nil
}

func (l *tcpListener) Close() error { return l.l.Close() }
func (l *tcpListener) Addr() string { return l.l.Addr().String() }

type tcpConn struct {
	c    net.Conn
	dec  *gob.Decoder
	out  *queue
	once sync.Once
}

func newTCPConn(c net.Conn) *tcpConn {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	t := &tcpConn{c: c, dec: gob.NewDecoder(bufio.NewReader(c)), out: newQueue()}
	go t.writeLoop()
	return t
}

func (t *tcpConn) writeLoop() {
	w := bufio.NewWriter(t.c)
	enc := gob.NewEncoder(w)
	for {
		v, ok := t.out.pop()
		if !ok {
			w.Flush()
			t.c.Close()
			return
		}
		if err := enc.Encode(&envelope{Msg: v}); err != nil {
			t.out.abort()
			t.c.Close()
			return
		}
		// Flush only when nothing else is queued, batching small messages.
		t.out.mu.Lock()
		empty := len(t.out.items) == 0
		t.out.mu.Unlock()
		if empty {
			if err := w.Flush(); err != nil {
				t.out.abort()
				t.c.Close()
				return
			}
		}
	}
}

func (t *tcpConn) Send(msg any) error {
	if !t.out.push(msg) {
		return ErrClosed
	}
	return nil
}

func (t *tcpConn) Recv() (any, error) {
	var e envelope
	if err := t.dec.Decode(&e); err != nil {
		t.Close()
		return nil, err
	}
	return e.Msg, nil
}

// Close flushes queued messages and closes the socket. If the peer does not
// drain the socket the connection is forcibly closed after a short delay.
func (t *tcpConn) Close() error {
	t.once.Do(func() {
		t.out.close()
		time.AfterFunc(time.Second, func() { t.c.Close() })
	})
	return nil
}
