package mux

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// NewRaftTransport creates new raft.NetworkTransport that uses Mux`s stream.
func NewRaftTransport(
	mux *Mux,
	stream byte,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) *raft.NetworkTransport {
	sl := &raftStreamLayer{
		listener: mux.Listen(stream),
		stream:   stream,
	}
	return raft.NewNetworkTransportWithLogger(sl, maxPool, timeout, logger)
}

// NewRaftTransportTLS creates new raft.NetworkTransport that uses Mux`s stream over TLS.
func NewRaftTransportTLS(
	mux *Mux,
	stream byte,
	maxPool int,
	timeout time.Duration,
	config *tls.Config,
	logger hclog.Logger,
) *raft.NetworkTransport {
	sl := &raftStreamLayer{
		listener:  mux.Listen(stream),
		stream:    stream,
		tlsConfig: config,
	}
	return raft.NewNetworkTransportWithLogger(sl, maxPool, timeout, logger)
}

type raftStreamLayer struct {
	listener  net.Listener
	stream    byte
	tlsConfig *tls.Config
}

func (l raftStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if l.tlsConfig != nil {
		return DialContextTLS(ctx, "tcp", string(address), l.stream, l.tlsConfig)
	}
	return DialContext(ctx, "tcp", string(address), l.stream)
}

func (l raftStreamLayer) Accept() (net.Conn, error) {
	return l.listener.Accept()
}

func (l raftStreamLayer) Close() error {
	return l.listener.Close()
}

func (l raftStreamLayer) Addr() net.Addr {
	return l.listener.Addr()
}
