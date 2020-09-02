package hooks

import pb "github.com/sysphusking/dsts/2pc/proto"

//自定义的Propose逻辑，可以自己实现并注入
func Propose(req *pb.ProposeRequest) bool {
	return true
}

//自定义的Commit逻辑，可以自己实现并注入
func Commit(req *pb.CommitRequest) bool {
	return true
}
