package mux

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Mux is a multiplexer for a net.Listener.
type Mux struct {
	ReadTimeout time.Duration

	innerLn net.Listener
	ln      net.Listener

	once    sync.Once
	wg      sync.WaitGroup
	running uint64

	handlers map[byte]*handler
}

const (
	DefaultReadTimeout = 30 * time.Second
	acceptTimeout      = 1 * time.Second
)

// Listen creates Mux using net.Listen.
func Listen(network string, address string) (*Mux, error) {
	ln, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}

	mux := &Mux{
		ReadTimeout: DefaultReadTimeout,
		innerLn:     ln,
		ln:          ln,
		handlers:    make(map[byte]*handler),
	}
	return mux, nil
}

// ListenTLS creates Mux using tls.Listen.
func ListenTLS(network string, address string, config *tls.Config) (*Mux, error) {
	ln, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	tlsln := tls.NewListener(ln, config)

	mux := &Mux{
		ReadTimeout: DefaultReadTimeout,
		innerLn:     ln,
		ln:          tlsln,
		handlers:    make(map[byte]*handler),
	}
	return mux, nil
}

// Close closes the multiplexer and waits for open connections to close.
func (mux *Mux) Close() (err error) {
	mux.once.Do(func() {
		atomic.StoreUint64(&mux.running, 0)

		// Close underlying listener
		if mux.ln != nil {
			err = mux.ln.Close()
		}

		// Wait for open connections to close and then close handlers
		mux.wg.Wait()
		for _, h := range mux.handlers {
			close(h.c)
		}
	})
	return
}

// Serve handles connections from ln and multiplexes then across registered listeners.
func (mux *Mux) Serve() error {
	atomic.StoreUint64(&mux.running, 1)
	for {
		// Handle incoming connection
		if ln, ok := mux.innerLn.(*net.TCPListener); ok {
			_ = ln.SetDeadline(time.Now().Add(acceptTimeout))
		} else {
			panic("unexpected listener type")
		}
		conn, err := mux.ln.Accept()

		// If multiplexer is closing - disregard error and exit
		if atomic.LoadUint64(&mux.running) == 0 {
			return nil
		}

		if err != nil {
			// On timeout - continue listening
			if err, ok := err.(net.Error); ok { //nolint:errorlint // We know the error is not wraped
				if err.Timeout() {
					continue
				}
			}

			// On other error - close
			_ = mux.Close()
			return err
		}

		// Hand off connection to a separate goroutine
		mux.wg.Add(1)
		go func(conn net.Conn) {
			defer mux.wg.Done()
			if err := mux.handleConn(conn); err != nil {
				_ = conn.Close()
			}
		}(conn)
	}
}

// handleConn handles incoming connection to the registered listener.
func (mux *Mux) handleConn(conn net.Conn) error {
	// Set a read deadline so connections with no data timeout
	if err := conn.SetReadDeadline(time.Now().Add(mux.ReadTimeout)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}

	// Read the first byte from connection to determine handler
	var typ [1]byte
	_, err := conn.Read(typ[:])
	if err != nil {
		return fmt.Errorf("read stream byte: %w", err)
	}

	// Reset read deadline and let the listener handle that
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("reset set read deadline: %w", err)
	}

	// Lookup handler
	h := mux.handlers[typ[0]]
	if h == nil {
		return fmt.Errorf("%w: 0x%02x", ErrUnknownStream, typ[0])
	}

	// Hand off connection to handler
	h.c <- conn
	return nil
}

// Listen returns a listener that receives connections for specified stream.
func (mux *Mux) Listen(stream byte) net.Listener {
	if atomic.LoadUint64(&mux.running) == 1 {
		panic("listen called after serve")
	}
	h := &handler{
		addr: mux.ln.Addr(),
		c:    make(chan net.Conn),
	}
	mux.handlers[stream] = h
	return h
}

// Addr returns the multiplexer's network address.
func (mux *Mux) Addr() net.Addr {
	return mux.ln.Addr()
}

// handler is a receiver for connections received by Mux. Implements net.Listener.
type handler struct {
	addr net.Addr
	c    chan net.Conn
}

// Accept waits for and returns the next connection.
func (h *handler) Accept() (c net.Conn, err error) {
	select {
	case conn, ok := <-h.c:
		if !ok {
			return nil, ErrConnectionClosed
		}
		return conn, nil
	case <-time.After(acceptTimeout):
		return nil, ErrConnectionClosed
	}
}

// Close implements net.Listener.
func (h *handler) Close() error {
	return nil
}

// Addr returns the address of the original listener.
func (h *handler) Addr() net.Addr { return h.addr }

var (
	ErrConnectionClosed = errors.New("network connection closed")
	ErrHandlerClosed    = errors.New("handler is already closed")
	ErrUnknownStream    = errors.New("unknown stream")
)
