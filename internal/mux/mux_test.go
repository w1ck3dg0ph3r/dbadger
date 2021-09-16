package mux_test

import (
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/w1ck3dg0ph3r/dbadger/internal/mux"
	"github.com/w1ck3dg0ph3r/dbadger/test"
)

func TestMux_TCP(t *testing.T) {
	t.Parallel()

	m, err := mux.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	m.ReadTimeout = 1 * time.Second
	defer m.Close()

	l1 := m.Listen(1)
	l2 := m.Listen(2)

	go func() {
		if err := m.Serve(); err != nil {
			t.Error(err)
		}
	}()

	go connectAndSendMessage(t, m, 1, "MSG1")
	go connectAndSendMessage(t, m, 2, "MSG2")

	acceptAndExpectMessage(t, l1, "MSG1")
	acceptAndExpectMessage(t, l2, "MSG2")
}

func TestMux_TLS(t *testing.T) {
	t.Parallel()

	configs := test.GenerateTestTLSConfigs([]string{"127.0.0.1"})
	assert.NotNilf(t, configs, "can't generate tls configs")
	for _, c := range configs {
		c.InsecureSkipVerify = true
	}

	m, err := mux.ListenTLS("tcp", "127.0.0.1:0", configs[0])
	assert.NoError(t, err)
	m.ReadTimeout = 1 * time.Second
	defer m.Close()

	l1 := m.Listen(1)
	l2 := m.Listen(2)

	go func() {
		if err := m.Serve(); err != nil {
			t.Error(err)
		}
	}()

	go connectAndSendMessageTLS(t, m, configs[0], 1, "MSG1")
	go connectAndSendMessageTLS(t, m, configs[0], 2, "MSG2")

	acceptAndExpectMessage(t, l1, "MSG1")
	acceptAndExpectMessage(t, l2, "MSG2")
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
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.FailNow()
	}

	m := mux.New(l)
	ml := m.Listen(1)

	go func() {
		if err := m.Serve(); err != nil {
			b.Fail()
		}
	}()

	go func() {
		for {
			conn, err := ml.Accept()
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
		conn, err := mux.Dial(l.Addr().Network(), l.Addr().String(), 1)
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

func connectAndSendMessage(t *testing.T, m *mux.Mux, stream byte, msg string) {
	t.Helper()
	conn, err := mux.Dial("tcp", m.Addr().String(), stream)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte(msg))
	assert.NoError(t, err)
}

func connectAndSendMessageTLS(t *testing.T, m *mux.Mux, cfg *tls.Config, stream byte, msg string) {
	t.Helper()
	conn, err := mux.DialTLS("tcp", m.Addr().String(), stream, cfg)
	assert.NoError(t, err)
	if conn == nil {
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(msg))
	assert.NoError(t, err)
}

func acceptAndExpectMessage(t *testing.T, l net.Listener, msg string) {
	t.Helper()
	connCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			errCh <- err
			return
		}
		connCh <- conn
	}()

	select {
	case <-time.NewTimer(1 * time.Second).C:
		assert.Fail(t, "accept timed out")
	case err := <-errCh:
		assert.NoError(t, err)
	case conn := <-connCh:
		defer conn.Close()
		buf, err := io.ReadAll(conn)
		assert.NoError(t, err)
		assert.Equal(t, []byte(msg), buf)
	}
}
