package hooks

import (
	pb "github.com/sysphusking/dsts/2pc/proto"
	"github.com/sysphusking/dsts/2pc/server"
)

type ProposeHook func(req *pb.ProposeRequest) bool
type CommitHook func(req *pb.CommitRequest) bool

//这里传入的路径没有用到
func Get() ([]server.Option, error) {

	//Option的用法，给server的ProposeHook赋值
	proposeHook := func(f ProposeHook) func(*server.Server) error {
		return func(server *server.Server) error {
			server.ProposeHook = f
			return nil
		}
	}
	commitHook := func(f CommitHook) func(*server.Server) error {

		return func(s *server.Server) error {
			s.CommitHook = f
			return nil
		}
	}

	return []server.Option{proposeHook(Propose), commitHook(Commit)}, nil
}
