package mux

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
)

// Dial connects to the stream of Mux listening on address on the named network.
func Dial(network string, address string, stream byte) (net.Conn, error) {
	return DialContext(context.Background(), network, address, stream)
}

// DialTLS connects to the stream of Mux listening on address on the named network over TLS.
func DialTLS(network string, address string, stream byte, config *tls.Config) (net.Conn, error) {
	return DialContextTLS(context.Background(), network, address, stream, config)
}

// DialContext acts like Dial but takes a context.
func DialContext(ctx context.Context, network string, address string, stream byte) (net.Conn, error) {
	// Create the connection
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	// Write stream byte to the connection
	n, err := conn.Write([]byte{stream})
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, ErrStreamByteWrite
	}

	return conn, nil
}

// DialContextTLS acts like DialTLS but takes a context.
func DialContextTLS(
	ctx context.Context,
	network string,
	address string,
	stream byte,
	config *tls.Config,
) (net.Conn, error) {
	// Create the connection
	host, _, _ := net.SplitHostPort(address)
	d := tls.Dialer{
		Config: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			ServerName:   host,
			RootCAs:      config.RootCAs,
			ClientCAs:    config.ClientCAs,
			Certificates: config.Certificates,
		},
	}
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	// Write stream byte to the connection
	n, err := conn.Write([]byte{stream})
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, ErrStreamByteWrite
	}

	return conn, nil
}

var ErrStreamByteWrite = errors.New("dial: stream byte not written")
