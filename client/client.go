package main

import (
	"fmt"
	"log"
	"net/rpc"
	raftmsg "raft/msg"
	"time"
)

func main() {
	host := "localhost"
	port := "4001"

	var client *rpc.Client
	var err error
	for {
		client, err = rpc.Dial("tcp", host+":"+port)
		if err != nil {
			log.Printf("error init client: %v\n", err)
		} else {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("connected to peer: %v", host+":"+port)
	msg := "Hello, I'm client"
	msgClientProposal := raftmsg.MsgClientProposal{
		Command: []byte(msg),
	}
	var msgClientProposalResponse raftmsg.MsgClientProposalResponse

	response := client.Go("RaftRPCService.ClientProposal", msgClientProposal, &msgClientProposalResponse, nil)

	<-response.Done
	fmt.Println("Success: ", msgClientProposalResponse.Success)
	fmt.Println("Leader: ", msgClientProposalResponse.Response)
}
