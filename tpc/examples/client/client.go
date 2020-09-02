package main

import (
	"context"
	"fmt"

	"github.com/sysphusking/dsts/2pc/client"

	pb "github.com/sysphusking/dsts/2pc/proto"
)

const addr = "localhost:3000"

func main() {

	cli, err := client.New(addr)
	if err != nil {
		panic(err)
	}
	resp, err := cli.Put(context.Background(), "1", []byte("2"))
	if err != nil {
		panic(err)
	}
	fmt.Println(resp)
	if resp.Type != pb.Type_ACK {
		panic("msg is not acknowledged")
	}
}
