package server

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/sysphusking/dsts/2pc/db"

	"github.com/sysphusking/dsts/2pc/cache"
	pb "github.com/sysphusking/dsts/2pc/proto"
)

func ProposeHandler(ctx context.Context, req *pb.ProposeRequest, hook func(req *pb.ProposeRequest) bool, nodeCache cache.ICache) (*pb.Response, error) {

	var rsp *pb.Response
	if hook(req) {
		log.Info(fmt.Sprintf("Propose Received: %s=%s\n", req.Key, string(req.Value)))
		nodeCache.Set(req.Index, req.Key, req.Value)
		rsp = &pb.Response{Type: pb.Type_ACK}
	} else {
		rsp = &pb.Response{Type: pb.Type_NACK}
	}
	return rsp, nil
}

func PreCommitHandler(ctx context.Context, req *pb.PrecommitRequest) (*pb.Response, error) {
	return &pb.Response{
		Type: pb.Type_ACK,
	}, nil
}

func CommitHandler(ctx context.Context, req *pb.CommitRequest, hook func(req *pb.CommitRequest) bool, db db.Database, nodeCache cache.ICache) (*pb.Response, error) {
	var rsp *pb.Response
	if hook(req) {
		log.Info(fmt.Sprintf("Committing on height: %d\n", req.Index))
		key, value, ok := nodeCache.Get(req.Index)
		if !ok {
			nodeCache.Delete(req.Index)
			return &pb.Response{Type: pb.Type_NACK}, errors.New(fmt.Sprintf("no value in node cache on the index %d", req.Index))
		}
		if err := db.Put(key, value); err != nil {
			return nil, err
		}
		rsp = &pb.Response{Type: pb.Type_ACK}

	} else {
		nodeCache.Delete(req.Index)
		rsp = &pb.Response{Type: pb.Type_NACK}
	}
	return rsp, nil
}
