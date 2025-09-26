package server

import (
	"net"
	"net/rpc"
	"raft/node"
	svc "raft/service"
)

type Server struct {
	rpc  rpc.Server
	node *node.Node
}

func NewServer(node *node.Node, rpc rpc.Server) *Server {
	return &Server{
		node: node,
		rpc:  rpc,
	}
}

func (srv *Server) RegisterServices() error {
	raftRPCService := svc.NewRaftRPCService(srv.node)
	err := srv.rpc.Register(raftRPCService)
	return err
}

func (srv *Server) Run() error {
	srv.RegisterServices()

	l, err := net.Listen("tcp", srv.node.NodeAddr)
	if err != nil {
		return err
	}
	defer l.Close()

	srv.rpc.Accept(l)
	return nil
}
