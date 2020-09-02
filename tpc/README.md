## tpc

两阶段提交和三阶段提交demo

编译pb文件，在tpc目录下执行，go_out的路径是`./proto`，
```shell script
protoc -I ./proto  --go_out=plugins=grpc:./proto   ./proto/mtpc.proto

```

tree phase commit那里有些地方设计的不是很好，本工程提供简单的demo仅供学习参考。


运行方式：
- make prepare
- make run-example-follower
- make run-example-coordinator
- make run-example-client

备注：需提前在config文件里配置下db信息
