package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sysphusking/dsts/2pc/server"

	"github.com/sysphusking/dsts/2pc/config"
	"github.com/sysphusking/dsts/2pc/hooks"
)

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	conf := config.Get()

	hooks, err := hooks.Get()
	if err != nil {
		panic(err)
	}

	s, err := server.NewCommitServer(conf, hooks...)
	if err != nil {
		panic(err)
	}
	s.Run()
	<-ch
	s.Stop()

}
