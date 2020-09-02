package server

import (
	"fmt"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/sysphusking/dsts/2pc/cache"
	"github.com/sysphusking/dsts/2pc/client"
	"github.com/sysphusking/dsts/2pc/config"
	"github.com/sysphusking/dsts/2pc/db"
	pb "github.com/sysphusking/dsts/2pc/proto"
	"google.golang.org/grpc"
)

const (
	TWO_PHASE   = "two-phase"
	THREE_PHASE = "three-phase"
)

type Option func(server *Server) error

type Server struct {
	Addr                 string
	Followers            []*client.CommitClient
	Config               *config.Config
	GrpcServer           *grpc.Server
	DB                   db.Database
	ProposeHook          func(req *pb.ProposeRequest) bool
	CommitHook           func(req *pb.CommitRequest) bool
	NodeCache            cache.ICache
	Height               uint64
	cancelCommitOnHeight map[uint64]bool
	mu                   sync.RWMutex
}

func (s *Server) SetCancelCache(height uint64, doCancel bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelCommitOnHeight[height] = doCancel
}

func (s *Server) GetCancelCache(height uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelCommitOnHeight[height]
}

func (s *Server) rollback() {
	s.NodeCache.Delete(s.Height)
}

func NewCommitServer(conf *config.Config, opts ...Option) (*Server, error) {
	//这里用的是logrus，我们用zap替换了
	//log.SetFormatter(&log.TextFormatter{
	//	ForceColors:     true, // Seems like automatic color detection doesn't work on windows terminals
	//	FullTimestamp:   true,
	//	TimestampFormat: time.RFC822,
	//})
	server := &Server{
		Addr: conf.NodeAddr,
	}
	var err error
	for _, opt := range opts {
		err = opt(server)
		if err != nil {
			return nil, err
		}
	}

	for _, node := range conf.Followers {
		cli, err := client.New(node)
		if err != nil {
			return nil, err
		}
		//将所有参与者加进来
		server.Followers = append(server.Followers, cli)
	}
	server.Config = conf
	if conf.Role == "coordinator" {
		server.Config.Coordinator = server.Addr
	}

	server.DB, err = db.New(viper.GetString("db.address"),
		viper.GetString("db.username"), viper.GetString("db.password"))

	server.NodeCache = cache.New()
	server.cancelCommitOnHeight = map[uint64]bool{}

	if server.Config.CommitType == TWO_PHASE {
		log.Info("two phase commit enabled")
	} else {
		log.Info("three phase commit enabled")
	}

	return server, nil
}

func (s *Server) Stop() {
	log.Info("Stopping server")
	s.GrpcServer.GracefulStop()
	if err := s.DB.Close(); err != nil {
		log.Info("failed to close db ,err : ", zap.Error(err))
	}
	log.Info("server stopped")
}

func (s *Server) Run(opts ...grpc.UnaryServerInterceptor) {
	var err error
	s.GrpcServer = grpc.NewServer(grpc.ChainUnaryInterceptor(opts...))
	pb.RegisterCommitServer(s.GrpcServer, s)
	l, err := net.Listen("tcp", s.Addr)

	if err != nil {
		log.Info("failed to listen, err: ", zap.Error(err))
	}

	log.Info(fmt.Sprintf("listening on tcp://%s", s.Addr))
	go s.GrpcServer.Serve(l)
}
