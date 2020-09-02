package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/metadata"

	log "github.com/sirupsen/logrus"

	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/sysphusking/dsts/2pc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Propose(ctx context.Context, request *pb.ProposeRequest) (*pb.Response, error) {
	s.SetCancelCache(request.Index, false)
	return ProposeHandler(ctx, request, s.ProposeHook, s.NodeCache)
}

func (s *Server) Precommit(ctx context.Context, request *pb.PrecommitRequest) (*pb.Response, error) {

	if s.Config.CommitType == THREE_PHASE {
		//设置超时
		ctx, _ = context.WithTimeout(context.Background(), time.Duration(s.Config.Timeout)*time.Millisecond)
		//总体来说这个机制不是很好，超时了没法用，比较鸡肋
		go func(ctx context.Context) {
		ForLoop:
			for {
				select {
				//超时后自动执行
				case <-ctx.Done():
					md := metadata.Pairs("mode", "autocommit")
					ctx := metadata.NewOutgoingContext(context.Background(), md)
					//这里的超时机制不会执行到CommitHandler里，不知道存在意义是什么
					if !s.GetCancelCache(request.Index) {
						s.Commit(ctx, &pb.CommitRequest{Index: s.Height})
						log.Info("commit without coordinator after timeout ")
					}
					break ForLoop
				}
			}
		}(ctx)
	}

	return PreCommitHandler(ctx, request)
}

func (s *Server) Commit(ctx context.Context, request *pb.CommitRequest) (resp *pb.Response, err error) {

	if s.Config.CommitType == THREE_PHASE {
		md, ok := metadata.FromIncomingContext(ctx)
		//log.Info(fmt.Sprintf("commit md: %v, %v", md, ok))
		if !ok {
			return nil, status.Errorf(codes.Internal, "no metadata")
		}
		meta := md["mode"]
		//如果没有设置autocommit，则是正常提交
		if len(meta) == 0 {
			//设置成true是不让preCommit中的协程进行重复的commit调用
			fmt.Println("bbbb")
			s.SetCancelCache(s.Height, true)
			resp, err = CommitHandler(ctx, request, s.CommitHook, s.DB, s.NodeCache)
			if err != nil {
				return nil, err
			}
			if resp.Type == pb.Type_ACK {
				atomic.AddUint64(&s.Height, 1)
			}
			//这里不写是在返回里有声明了
			return
		}

		//本次请求是否回滚
		if request.IsRollback {
			s.rollback()
		}
	} else {
		resp, err = CommitHandler(ctx, request, s.CommitHook, s.DB, s.NodeCache)
		if err != nil {
			return
		}
		if resp.Type == pb.Type_ACK {
			atomic.AddUint64(&s.Height, 1)
		}
		return
	}
	return
}

func (s *Server) Put(ctx context.Context, entry *pb.Entry) (*pb.Response, error) {

	var (
		response *pb.Response
		err      error
	)
	var ctype pb.CommitType
	if s.Config.CommitType == THREE_PHASE {
		ctype = pb.CommitType_THREE_PHASE_COMMIT
	} else {
		ctype = pb.CommitType_TWO_PHASE_COMMIT
	}

	//propose
	s.NodeCache.Set(s.Height, entry.Key, entry.Value)
	for _, follower := range s.Followers {
		if s.Config.CommitType == THREE_PHASE {
			ctx, _ = context.WithTimeout(ctx, time.Duration(s.Config.Timeout)*time.Millisecond)
		}
		response, err = follower.Propose(ctx, &pb.ProposeRequest{
			Key:        entry.Key,
			Value:      entry.Value,
			CommitType: ctype,
			Index:      s.Height,
		})
		if err != nil {
			log.Error(err.Error())
			return &pb.Response{Type: pb.Type_NACK}, nil
		}
		if response.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}

	//preCommit
	for _, follower := range s.Followers {
		if s.Config.CommitType == THREE_PHASE {
			ctx, _ = context.WithTimeout(ctx, time.Duration(s.Config.Timeout)*time.Millisecond)
		}
		response, err = follower.Precommit(ctx, &pb.PrecommitRequest{Index: s.Height})
		if err != nil {
			return &pb.Response{Type: pb.Type_NACK}, nil
		}
		if response.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}

	//从cache中获取key和value
	key, value, ok := s.NodeCache.Get(s.Height)
	if !ok {
		return nil, status.Error(codes.Internal, "can't to find msg in the coordinator's cache")
	}
	//将数据存储起来，coordinator会保存一份，follower也会保存一份
	if err = s.DB.Put(key, value); err != nil {
		return &pb.Response{Type: pb.Type_NACK}, status.Error(codes.Internal, "failed to save msg on coordinator")
	}

	//这里是有问题的，如果上面两步操作有延迟，那么请求就会失败。需要指定超时时间，这点设计的很不好
	//time.Sleep(time.Duration(100) * time.Millisecond)

	//commit
	for _, follower := range s.Followers {
		//commit的逻辑失败的话需要回滚，这个操作由follower自己实现
		response, err = follower.Commit(ctx, &pb.CommitRequest{Index: s.Height})
		if err != nil {
			return &pb.Response{Type: pb.Type_NACK}, nil
		}
		if response.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}

	atomic.AddUint64(&s.Height, 1)

	return &pb.Response{
		Type: pb.Type_ACK,
	}, nil
}

func (s *Server) Get(ctx context.Context, msg *pb.Msg) (*pb.Value, error) {
	value, err := s.DB.Get(msg.Key)
	if err != nil {
		return nil, err
	}
	return &pb.Value{
		Value: value,
	}, nil
}

func (s *Server) NodeInfo(ctx context.Context, empty *empty.Empty) (*pb.Info, error) {
	return &pb.Info{
		Height: s.Height,
	}, nil
}
