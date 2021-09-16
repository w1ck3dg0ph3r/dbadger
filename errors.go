package dbadger

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/w1ck3dg0ph3r/dbadger/internal/rpc"
)

// Errors that can be returned from [DB] methods.
var (
	// ErrNoLeader is returned if a cluster has no leader or the leader is unknown at the moment.
	ErrNoLeader = errors.New("cluster has no leader")

	// ErrEmptyKey is returned if an empty key is passed to a write operation.
	ErrEmptyKey = errors.New("key can not be empty")
	// ErrInvalidKey is returned if the key has a special !badger! prefix, reserved for internal usage.
	ErrInvalidKey = errors.New("key is using a reserved !badger! prefix")
	// ErrNotFound is returned when key is not found in read operations.
	ErrNotFound = errors.New("key not found")

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("invalid request")

	// ErrConflict is returned when a transaction conflicts with another transaction. This can
	// happen if the read rows had been updated concurrently by another transaction.
	ErrConflict = errors.New("transaction conflict, please retry")

	// ErrUnavailable is returned when the cluster is currently unable to perform the
	// requested operation, but the operation should be retried with a backoff.
	// This can mean network errors, blocked writes during DeleteAll, etc.
	ErrUnavailable = errors.New("operation is currently unavailable, please retry")

	ErrInternal = errors.New("internal error")
)

// GRPC status errors.
//
//nolint:errname,gochecknoglobals
var (
	statusEmptyKey       = newGRPCError(codes.InvalidArgument, ErrEmptyKey.Error(), rpc.Error_EMPTY_KEY)
	statusInvalidKey     = newGRPCError(codes.InvalidArgument, ErrInvalidKey.Error(), rpc.Error_INVALID_KEY)
	statusNotFound       = newGRPCError(codes.NotFound, ErrNotFound.Error(), rpc.Error_NOT_FOUND)
	statusInvalidRequest = newGRPCError(codes.InvalidArgument, ErrInvalidRequest.Error(), rpc.Error_INVALID_REQUEST)
	statusConflict       = newGRPCError(codes.Unavailable, ErrConflict.Error(), rpc.Error_CONFLICT)
	statusUnavailable    = newGRPCError(codes.Unavailable, ErrUnavailable.Error(), rpc.Error_UNAVAILABLE)
)

func newGRPCError(rpcCode codes.Code, msg string, code rpc.Error_Code) error {
	st, err := status.New(rpcCode, msg).WithDetails(&rpc.Error{Code: code})
	if err != nil {
		panic(err)
	}
	return st.Err()
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		if len(st.Details()) > 0 {
			if e, ok := st.Details()[0].(*rpc.Error); ok {
				switch e.Code {
				case rpc.Error_NO_LEADER:
					return ErrNoLeader
				case rpc.Error_EMPTY_KEY:
					return ErrEmptyKey
				case rpc.Error_INVALID_KEY:
					return ErrInvalidKey
				case rpc.Error_NOT_FOUND:
					return ErrNotFound
				case rpc.Error_INVALID_REQUEST:
					return ErrInvalidRequest
				case rpc.Error_CONFLICT:
					return ErrConflict
				case rpc.Error_UNAVAILABLE:
					return ErrUnavailable
				}
			}
		}
		switch st.Code() {
		case codes.Unavailable,
			codes.Canceled:
			return fmt.Errorf("%w: %v", ErrUnavailable, err)
		default:
			return fmt.Errorf("%w: %v", ErrInternal, err)
		}
	}
	return err
}
