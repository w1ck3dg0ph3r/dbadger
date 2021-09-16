package mux

import (
	"context"
	"crypto/tls"
	"net"

	"google.golang.org/grpc"
)

// NewGRPCConn creates new grpc.ClientConn that uses Dial for initiating connections.
func NewGRPCConn(target string, stream byte, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append(opts,
		grpc.WithContextDialer(
			func(ctx context.Context, address string) (net.Conn, error) {
				return DialContext(ctx, "tcp", address, stream)
			},
		),
	)
	return grpc.Dial(target, opts...)
}

// NewGRPCConnTLS creates new grpc.ClientConn that uses DialTLS for initiating connections.
func NewGRPCConnTLS(target string, stream byte, config *tls.Config, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append(opts,
		grpc.WithContextDialer(
			func(ctx context.Context, address string) (net.Conn, error) {
				return DialContextTLS(ctx, "tcp", address, stream, config)
			},
		),
	)
	return grpc.Dial(target, opts...)
}
