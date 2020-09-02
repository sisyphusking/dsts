package client

import (
	"context"

	"github.com/pkg/errors"

	"google.golang.org/grpc"

	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/sysphusking/dsts/2pc/proto"
)

type CommitClient struct {
	Connection pb.CommitClient
}

func New(addr string) (*CommitClient, error) {
	conn, err := grpc.Dial(
		addr,
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect")
	}
	return &CommitClient{
		Connection: pb.NewCommitClient(conn),
	}, nil
}

func (c *CommitClient) Propose(ctx context.Context, in *pb.ProposeRequest) (*pb.Response, error) {
	return c.Connection.Propose(ctx, in)
}

func (c *CommitClient) Precommit(ctx context.Context, in *pb.PrecommitRequest) (*pb.Response, error) {
	return c.Connection.Precommit(ctx, in)
}

func (c *CommitClient) Commit(ctx context.Context, in *pb.CommitRequest) (*pb.Response, error) {
	return c.Connection.Commit(ctx, in)
}

func (c *CommitClient) Put(ctx context.Context, key string, value []byte) (*pb.Response, error) {
	return c.Connection.Put(ctx, &pb.Entry{
		Key:   key,
		Value: value,
	})
}

func (c *CommitClient) Get(ctx context.Context, key string) (*pb.Value, error) {
	return c.Connection.Get(ctx, &pb.Msg{Key: key})
}

func (c *CommitClient) NodeInfo(ctx context.Context) (*pb.Info, error) {
	return c.Connection.NodeInfo(ctx, &empty.Empty{})
}
