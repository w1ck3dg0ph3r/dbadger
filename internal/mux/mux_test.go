package mux_test

import (
	"crypto/tls"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/w1ck3dg0ph3r/dbadger/internal/mux"
	"github.com/w1ck3dg0ph3r/dbadger/test"
)

func TestMux_TCP(t *testing.T) {
	m, err := mux.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	l1 := m.Listen(1)
	l2 := m.Listen(2)

	closed := make(chan struct{})
	go func() {
		if err := m.Serve(); err != nil {
			t.Error(err)
		}
		close(closed)
	}()

	go connectAndSendMessage(t, m, 1, "MSG1")
	go connectAndSendMessage(t, m, 2, "MSG2")

	acceptAndExpectMessage(t, l1, "MSG1")
	acceptAndExpectMessage(t, l2, "MSG2")

	m.Close()
	<-closed
}

func TestMux_TLS(t *testing.T) {
	configs := test.GenerateTestTLSConfigs([]string{"127.0.0.1"})
	assert.NotNilf(t, configs, "can't generate tls configs")
	for _, c := range configs {
		c.InsecureSkipVerify = true
	}

	m, err := mux.ListenTLS("tcp", "127.0.0.1:0", configs[0])
	assert.NoError(t, err)

	l1 := m.Listen(1)
	l2 := m.Listen(2)

	closed := make(chan struct{})
	go func() {
		if err := m.Serve(); err != nil {
			t.Error(err)
		}
		close(closed)
	}()

	go connectAndSendMessageTLS(t, m, configs[0], 1, "MSG1")
	go connectAndSendMessageTLS(t, m, configs[0], 2, "MSG2")

	acceptAndExpectMessage(t, l1, "MSG1")
	acceptAndExpectMessage(t, l2, "MSG2")

	m.Close()
	<-closed
}

func TestMux_Racing(t *testing.T) {
	m, err := mux.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	m.ReadTimeout = 3 * time.Second
	m.AcceptTimeout = 3 * time.Second

	const numStreams = 4
	const numMessages = 1000

	// Create listeners
	listeners := make([]net.Listener, numStreams)
	for i := byte(0); i < numStreams; i++ {
		listeners[i] = m.Listen(i)
	}

	// Start mux
	closed := make(chan struct{})
	go func() {
		err := m.Serve()
		assert.NoError(t, err)
		close(closed)
	}()

	message := "MESSAGE"
	var wg sync.WaitGroup
	wg.Add(numStreams * 2)
	for i := 0; i < numStreams; i++ {
		i := i
		go func() {
			for msg := 0; msg < numMessages; msg++ {
				acceptAndExpectMessage(t, listeners[i], message)
			}
			wg.Done()
		}()
	}
	for i := byte(0); i < numStreams; i++ {
		i := i
		go func() {
			for msg := 0; msg < numMessages; msg++ {
				connectAndSendMessage(t, m, i, message)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	m.Close()
	<-closed
}

func TestMux_AcceptTimeout(t *testing.T) {
	m, err := mux.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	m.AcceptTimeout = 100 * time.Millisecond
	l := m.Listen(1)
	_, err = l.Accept()
	assert.ErrorIs(t, err, mux.ErrConnectionTimeout)
}

// go test -run=^$ -bench=. -benchmem ./internal/mux

// cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
// BenchmarkNetAccept-8   	   13708	    101290 ns/op	     948 B/op	      23 allocs/op
// BenchmarkMuxAccept-8   	    9002	    119678 ns/op	    1015 B/op	      27 allocs/op

func BenchmarkNetAccept(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.FailNow()
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}
			_, _ = conn.Write([]byte("TEST"))
			_ = conn.Close()
		}
	}()

	b.ResetTimer()
	var buf [4]byte
	for n := 0; n < b.N; n++ {
		conn, err := net.Dial(l.Addr().Network(), l.Addr().String())
		if err != nil {
			b.Fail()
		}
		_, err = conn.Read(buf[:])
		if err != nil {
			b.Fail()
		}
		conn.Close()
	}
}

func BenchmarkMuxAccept(b *testing.B) {
	m, err := mux.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.FailNow()
	}

	l := m.Listen(1)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		if err := m.Serve(); err != nil {
			b.Fail()
		}
	}()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}
			_, _ = conn.Write([]byte("TEST"))
			_ = conn.Close()
		}
	}()

	b.ResetTimer()
	var buf [4]byte
	for n := 0; n < b.N; n++ {
		conn, err := mux.Dial(m.Addr().Network(), m.Addr().String(), 1)
		if err != nil {
			b.Fail()
		}
		_, err = conn.Read(buf[:])
		if err != nil {
			b.Fail()
		}
		conn.Close()
	}

	m.Close()
	<-closed
}

func connectAndSendMessage(t *testing.T, m *mux.Mux, stream byte, msg string) {
	t.Helper()
	conn, err := mux.Dial("tcp", m.Addr().String(), stream)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	if conn == nil {
		return
	}
	defer conn.Close()

	n, err := conn.Write([]byte(msg))
	assert.NoError(t, err)
	assert.Equal(t, len([]byte(msg)), n)
}

func connectAndSendMessageTLS(t *testing.T, m *mux.Mux, cfg *tls.Config, stream byte, msg string) {
	t.Helper()
	conn, err := mux.DialTLS("tcp", m.Addr().String(), stream, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	if conn == nil {
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(msg))
	assert.NoError(t, err)
}

func acceptAndExpectMessage(t *testing.T, l net.Listener, msg string) {
	t.Helper()
	var conn net.Conn
	var err error
	for {
		conn, err = l.Accept()
		if err != nil {
			if err == mux.ErrConnectionTimeout {
				continue
			}
			return
		}
		break
	}

	assert.NoError(t, err)
	defer conn.Close()
	buf, err := io.ReadAll(conn)
	assert.NoError(t, err)
	assert.Equal(t, []byte(msg), buf)
}

func acceptAndExpectError(t *testing.T, l net.Listener, expectedErr error) {
	t.Helper()
	var conn net.Conn
	var err error
	for {
		conn, err = l.Accept()
		if err != nil {
			if err == mux.ErrConnectionTimeout {
				continue
			}
			return
		}
		break
	}

	assert.ErrorIs(t, err, expectedErr)
	defer conn.Close()
	_, err = io.ReadAll(conn)
	assert.NoError(t, err)
}
