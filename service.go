package dbadger

import (
	"context"

	"github.com/w1ck3dg0ph3r/dbadger/internal/rpc"
)

// service implements a RPC service for DB.
// service handles forwarding write commands to the leader node when necessary.
type service struct {
	*DB
	rpc.UnimplementedServiceServer
}

func (s *service) AddPeer(ctx context.Context, peer *rpc.CommandAddPeer) (*rpc.ResultAddPeer, error) {
	if fwd, err := s.shouldForwardToLeader(); err != nil {
		return nil, err
	} else if fwd {
		return s.grpcLeader().AddPeer(ctx, peer)
	}
	err := s.executor.AddPeer(ctx, peer.Addr)
	if err != nil {
		return nil, err
	}
	return &rpc.ResultAddPeer{}, nil
}

func (s *service) RemovePeer(ctx context.Context, peer *rpc.CommandRemovePeer) (*rpc.ResultRemovePeer, error) {
	if fwd, err := s.shouldForwardToLeader(); err != nil {
		return nil, err
	} else if fwd {
		return s.grpcLeader().RemovePeer(ctx, peer)
	}
	err := s.executor.RemovePeer(ctx, peer.Addr)
	if err != nil {
		return nil, err
	}
	return &rpc.ResultRemovePeer{}, err
}

func (s *service) Get(ctx context.Context, cmd *rpc.CommandGet) (*rpc.ResultGet, error) {
	if cmd.ReadPreference == rpc.ReadPreference_LEADER {
		if fwd, err := s.shouldForwardToLeader(); err != nil {
			return nil, err
		} else if fwd {
			return s.grpcLeader().Get(ctx, cmd)
		}
	}
	return s.executor.Get(ctx, cmd)
}

func (s *service) GetMany(ctx context.Context, cmd *rpc.CommandGetMany) (*rpc.ResultGetMany, error) {
	if cmd.ReadPreference == rpc.ReadPreference_LEADER {
		if fwd, err := s.shouldForwardToLeader(); err != nil {
			return nil, err
		} else if fwd {
			return s.grpcLeader().GetMany(ctx, cmd)
		}
	}
	return s.executor.GetMany(ctx, cmd)
}

func (s *service) GetPrefix(ctx context.Context, cmd *rpc.CommandGetPrefix) (*rpc.ResultGetPrefix, error) {
	if cmd.ReadPreference == rpc.ReadPreference_LEADER {
		if fwd, err := s.shouldForwardToLeader(); err != nil {
			return nil, err
		} else if fwd {
			return s.grpcLeader().GetPrefix(ctx, cmd)
		}
	}
	return s.executor.GetPrefix(ctx, cmd)
}

func (s *service) GetRange(ctx context.Context, cmd *rpc.CommandGetRange) (*rpc.ResultGetRange, error) {
	if cmd.ReadPreference == rpc.ReadPreference_LEADER {
		if fwd, err := s.shouldForwardToLeader(); err != nil {
			return nil, err
		} else if fwd {
			return s.grpcLeader().GetRange(ctx, cmd)
		}
	}
	return s.executor.GetRange(ctx, cmd)
}

func (s *service) Set(ctx context.Context, cmd *rpc.CommandSet) (*rpc.ResultSet, error) {
	if fwd, err := s.shouldForwardToLeader(); err != nil {
		return nil, err
	} else if fwd {
		return s.grpcLeader().Set(ctx, cmd)
	} else {
		return s.executor.Set(ctx, cmd)
	}
}

func (s *service) SetMany(ctx context.Context, cmd *rpc.CommandSetMany) (*rpc.ResultSetMany, error) {
	if fwd, err := s.shouldForwardToLeader(); err != nil {
		return nil, err
	} else if fwd {
		return s.grpcLeader().SetMany(ctx, cmd)
	} else {
		return s.executor.SetMany(ctx, cmd)
	}
}

func (s *service) Delete(ctx context.Context, cmd *rpc.CommandDelete) (*rpc.ResultDelete, error) {
	if fwd, err := s.shouldForwardToLeader(); err != nil {
		return nil, err
	} else if fwd {
		return s.grpcLeader().Delete(ctx, cmd)
	} else {
		return s.executor.Delete(ctx, cmd)
	}
}

func (s *service) DeleteMany(ctx context.Context, cmd *rpc.CommandDeleteMany) (*rpc.ResultDeleteMany, error) {
	if fwd, err := s.shouldForwardToLeader(); err != nil {
		return nil, err
	} else if fwd {
		return s.grpcLeader().DeleteMany(ctx, cmd)
	} else {
		return s.executor.DeleteMany(ctx, cmd)
	}
}

func (s *service) DeletePrefix(ctx context.Context, cmd *rpc.CommandDeletePrefix) (*rpc.ResultDeletePrefix, error) {
	if fwd, err := s.shouldForwardToLeader(); err != nil {
		return nil, err
	} else if fwd {
		return s.grpcLeader().DeletePrefix(ctx, cmd)
	} else {
		return s.executor.DeletePrefix(ctx, cmd)
	}
}

func (s *service) DeleteRange(ctx context.Context, cmd *rpc.CommandDeleteRange) (*rpc.ResultDeleteRange, error) {
	if fwd, err := s.shouldForwardToLeader(); err != nil {
		return nil, err
	} else if fwd {
		return s.grpcLeader().DeleteRange(ctx, cmd)
	} else {
		return s.executor.DeleteRange(ctx, cmd)
	}
}

func (s *service) DeleteAll(ctx context.Context, cmd *rpc.CommandDeleteAll) (*rpc.ResultDeleteAll, error) {
	if fwd, err := s.shouldForwardToLeader(); err != nil {
		return nil, err
	} else if fwd {
		return s.grpcLeader().DeleteAll(ctx, cmd)
	} else {
		return s.executor.DeleteAll(ctx, cmd)
	}
}

func (s *service) shouldForwardToLeader() (bool, error) {
	if s.raftNode.Leader() == "" {
		return true, ErrNoLeader
	}
	vl := s.raftNode.VerifyLeader()
	if err := vl.Error(); err != nil {
		if s.grpcLeader() != nil {
			return true, nil
		}
		return true, ErrNoLeader
	}
	return false, nil
}
