//nolint:forcetypeassert // Forced typed asserts are used to cast known RPC result types.
package dbadger

import (
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/w1ck3dg0ph3r/dbadger/internal/rpc"
)

// executor is responsible for executing RPC commands on a node.
type executor struct {
	*DB
}

const defaultTimeout = 3 * time.Second

func (e *executor) AddPeer(ctx context.Context, addr string) error {
	if err := e.raftNode.AddVoter(raft.ServerID(addr), raft.ServerAddress(addr), 0, timeout(ctx)).Error(); err != nil {
		return err
	}
	return nil
}

func (e *executor) RemovePeer(ctx context.Context, addr string) error {
	if err := e.raftNode.RemoveServer(raft.ServerID(addr), 0, timeout(ctx)).Error(); err != nil {
		return err
	}
	return nil
}

func (e *executor) Get(ctx context.Context, cmd *rpc.CommandGet) (*rpc.ResultGet, error) {
	res, err := e.executeRead(ctx, cmd.ReadPreference, func() (any, error) {
		value, err := e.data.Get(cmd.Key)
		return &rpc.ResultGet{Value: value}, err
	})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultGet), nil
}

func (e *executor) GetMany(ctx context.Context, cmd *rpc.CommandGetMany) (*rpc.ResultGetMany, error) {
	res, err := e.executeRead(ctx, cmd.ReadPreference, func() (any, error) {
		values, err := e.data.GetMany(cmd.Keys)
		return &rpc.ResultGetMany{Values: values}, err
	})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultGetMany), nil
}

func (e *executor) GetPrefix(ctx context.Context, cmd *rpc.CommandGetPrefix) (*rpc.ResultGetPrefix, error) {
	res, err := e.executeRead(ctx, cmd.ReadPreference, func() (any, error) {
		keys, values, err := e.data.GetPrefix(cmd.Prefix)
		return &rpc.ResultGetPrefix{Keys: keys, Values: values}, err
	})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultGetPrefix), nil
}

func (e *executor) GetRange(ctx context.Context, cmd *rpc.CommandGetRange) (*rpc.ResultGetRange, error) {
	res, err := e.executeRead(ctx, cmd.ReadPreference, func() (any, error) {
		keys, values, err := e.data.GetRange(cmd.Min, cmd.Max, int(cmd.Count))
		return &rpc.ResultGetRange{Keys: keys, Values: values}, err
	})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultGetRange), nil
}

func (e *executor) Set(ctx context.Context, cmd *rpc.CommandSet) (*rpc.ResultSet, error) {
	res, err := e.execCommand(ctx, &rpc.Command{Command: &rpc.Command_Set{Set: cmd}})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultSet), nil
}

func (e *executor) SetMany(ctx context.Context, cmd *rpc.CommandSetMany) (*rpc.ResultSetMany, error) {
	res, err := e.execCommand(ctx, &rpc.Command{Command: &rpc.Command_SetMany{SetMany: cmd}})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultSetMany), nil
}

func (e *executor) Delete(ctx context.Context, cmd *rpc.CommandDelete) (*rpc.ResultDelete, error) {
	res, err := e.execCommand(ctx, &rpc.Command{Command: &rpc.Command_Delete{Delete: cmd}})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultDelete), nil
}

func (e *executor) DeleteMany(ctx context.Context, cmd *rpc.CommandDeleteMany) (*rpc.ResultDeleteMany, error) {
	res, err := e.execCommand(ctx, &rpc.Command{Command: &rpc.Command_DeleteMany{DeleteMany: cmd}})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultDeleteMany), nil
}

func (e *executor) DeletePrefix(ctx context.Context, cmd *rpc.CommandDeletePrefix) (*rpc.ResultDeletePrefix, error) {
	res, err := e.execCommand(ctx, &rpc.Command{Command: &rpc.Command_DeletePrefix{DeletePrefix: cmd}})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultDeletePrefix), nil
}

func (e *executor) DeleteRange(ctx context.Context, cmd *rpc.CommandDeleteRange) (*rpc.ResultDeleteRange, error) {
	res, err := e.execCommand(ctx, &rpc.Command{Command: &rpc.Command_DeleteRange{DeleteRange: cmd}})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultDeleteRange), nil
}

func (e *executor) DeleteAll(ctx context.Context, cmd *rpc.CommandDeleteAll) (*rpc.ResultDeleteAll, error) {
	res, err := e.execCommand(ctx, &rpc.Command{Command: &rpc.Command_DeleteAll{DeleteAll: cmd}})
	if err != nil {
		return nil, err
	}
	return res.(*rpc.ResultDeleteAll), nil
}

func (e *executor) executeRead(ctx context.Context, readPreference rpc.ReadPreference, read func() (any, error)) (any, error) {
	if readPreference == rpc.ReadPreference_LEADER {
		e.raftNode.Barrier(0)
	}

	resCh := make(chan any, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := read()
		if err != nil {
			errCh <- errToGRPC(err)
			return
		}
		resCh <- res
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		return nil, err
	case res := <-resCh:
		return res, nil
	}
}

func (e *executor) execCommand(ctx context.Context, cmd *rpc.Command) (any, error) {
	buf, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	f := e.raftNode.Apply(buf, timeout(ctx))
	if err := f.Error(); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	res := f.Response()
	if err, ok := res.(error); ok {
		return nil, errToGRPC(err)
	}
	return res, nil
}

func errToGRPC(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return statusNotFound
	case errors.Is(err, badger.ErrConflict):
		return statusConflict
	case errors.Is(err, badger.ErrBlockedWrites):
		return statusUnavailable
	case errors.Is(err, badger.ErrInvalidKey):
		return statusInvalidKey
	case errors.Is(err, badger.ErrEmptyKey):
		return statusEmptyKey
	case errors.Is(err, badger.ErrInvalidRequest):
		return statusInvalidRequest
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func timeout(ctx context.Context) time.Duration {
	dl, ok := ctx.Deadline()
	if ok {
		return time.Until(dl)
	}
	return defaultTimeout
}
