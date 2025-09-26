package service

import (
	"log"
	"raft/msg"
	"raft/node"
	"time"
)

type RaftRPCService struct {
	node *node.Node
}

func NewRaftRPCService(node *node.Node) *RaftRPCService {
	return &RaftRPCService{
		node: node,
	}
}

func (rs *RaftRPCService) RequestVote(req msg.MsgRequestVote, reply *msg.MsgRequestVoteResponse) error {
	log.Printf("received RequestVote rpc from node id: %v, for term: %v", req.CandidateID, req.TermID)
	rs.node.Mu.Lock()
	defer rs.node.Mu.Unlock()

	if req.TermID > rs.node.TermID {
		rs.node.TermID = req.TermID
		rs.node.NodeState = node.FOLLOWER
		rs.node.LastVotedFor = -1
	}

	reply.TermID = rs.node.TermID
	if req.TermID < rs.node.TermID {
		reply.Vote = false
		return nil
	}

	// check if already voted for this term
	if rs.node.LastVotedFor != req.CandidateID && rs.node.LastVotedFor != -1 {
		reply.Vote = false
		return nil
	}

	// check if candidate is up to date
	termID, msgID := rs.node.LatestRecord()
	if termID > req.LatestRecordTermID || (termID == req.LatestRecordTermID && msgID > req.LatestRecordMsgID) {
		reply.Vote = false
		return nil
	}
	reply.Vote = true
	rs.node.LastVotedFor = req.CandidateID
	rs.node.TermID = req.TermID
	reply.TermID = req.TermID

	return nil
}

func (rs *RaftRPCService) AppendEntries(msgAppendEntries msg.MsgAppendEntries, reply *msg.MsgAppendEntriesResponse) error {
	log.Printf("received AppendEntries rpc from node id: %v", msgAppendEntries.LeaderID)
	rs.node.Mu.Lock()
	defer rs.node.Mu.Unlock()
	rs.node.LeaderPingTimestamp = time.Now()
	if rs.node.NodeState == node.LEADER {
		go rs.node.StepDownFromLeader()
	}
	rs.node.NodeState = node.FOLLOWER
	rs.node.TermID = msgAppendEntries.TermID
	return nil
}
