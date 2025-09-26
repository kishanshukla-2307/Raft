package main

import (
	"context"
	"flag"
	"net/rpc"
	"os"
	"os/signal"
	"raft/node"
	"raft/server"
	"strings"
)

var (
	nodeId   = flag.Int("id", 1, "ID for the node")
	nodeHost = flag.String("host", "localhost", "Host name for the node")
	nodePort = flag.String("port", "8080", "port for the node")
	peersStr = flag.String("peers", "localhost:8081", "Peers addresses")
)

func main() {
	flag.Parse()

	peers := strings.Split(*peersStr, ",")
	// peers := make([]string, 0)
	nodeAddr := *nodeHost + ":" + *nodePort
	node := node.NewNode(*nodeId, nodeAddr, peers)

	srv := server.NewServer(node, *rpc.NewServer())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go RunServer(srv)
	go node.Run(ctx)

	<-ctx.Done()
}

func RunServer(srv *server.Server) {
	if err := srv.Run(); err != nil {
		panic(err)
	}
}
