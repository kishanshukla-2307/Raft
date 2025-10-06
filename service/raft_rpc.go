package service

import (
	"log"
	"raft/msg"
	"raft/node"
	"raft/types"
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

func (rs *RaftRPCService) RequestVote(req msg.MsgRequestVote, response *msg.MsgRequestVoteResponse) error {
	rs.node.Mu.Lock()
	defer rs.node.Mu.Unlock()

	log.Printf("received RequestVote rpc from node id: %v, for term: %v", req.CandidateID, req.TermID)

	if req.TermID > rs.node.TermID {
		rs.node.TermID = req.TermID
		rs.node.BecomeFollower()
		rs.node.LastVotedFor = -1
	}
	response.TermID = rs.node.TermID

	if req.TermID < rs.node.TermID {
		response.Vote = false
		return nil
	}

	// check if already voted someone else for this term
	if rs.node.LastVotedFor != req.CandidateID && rs.node.LastVotedFor != -1 {
		response.Vote = false
		return nil
	}

	// check if candidate is up to date
	termID, msgID := rs.node.LatestRecord()
	if termID > req.LatestRecordTermID || (termID == req.LatestRecordTermID && msgID > req.LatestRecordMsgID) {
		response.Vote = false
		return nil
	}
	response.Vote = true
	rs.node.LastVotedFor = req.CandidateID
	rs.node.TermID = req.TermID
	response.TermID = req.TermID
	return nil
}

func (rs *RaftRPCService) AppendEntries(req msg.MsgAppendEntries, response *msg.MsgAppendEntriesResponse) error {
	rs.node.Mu.Lock()
	defer rs.node.Mu.Unlock()

	log.Printf("received AppendEntries rpc with %v entries from node id: %v, Term: %v", len(req.Entries), req.LeaderID, req.TermID)

	if req.TermID < rs.node.TermID {
		response.Success = false
		response.TermID = rs.node.TermID
		return nil
	}
	if req.TermID > rs.node.TermID {
		rs.node.TermID = req.TermID
		rs.node.BecomeFollower()
	}
	rs.node.LeaderPingTimestamp = time.Now()
	if req.LastEntryMsgID >= 0 {
		if req.LastEntryMsgID >= types.MsgID(rs.node.Log.GetLength()) {
			response.Success = false
			response.TermID = rs.node.TermID
			return nil
		}
		entry := rs.node.Log.GetEntry(req.LastEntryMsgID)
		if entry.TermID != req.LastEntryTermID {
			response.Success = false
			response.TermID = rs.node.TermID
			return nil
		}
	}

	for i := range req.Entries {
		idx := i + int(req.LastEntryMsgID) + 1
		if idx < rs.node.Log.GetLength() {
			if !rs.node.Log.GetEntry(types.MsgID(idx)).Equal(&req.Entries[i]) {
				rs.node.Log.DiscardEntries(types.MsgID(idx))
				rs.node.Log.AppendEntry(req.Entries[i].TermID, req.Entries[i].Command)
			}
		} else {
			rs.node.Log.AppendEntry(req.Entries[i].TermID, req.Entries[i].Command)
		}
	}

	response.Success = true
	response.TermID = rs.node.TermID
	rs.node.CrrLeader = req.LeaderID
	if req.LeaderCommit > rs.node.CommitIdx {
		rs.node.CommitIdx = min(req.LeaderCommit, rs.node.Log.GetLength()-1)
	}
	return nil
}

func (rs *RaftRPCService) ClientProposal(req msg.MsgClientProposal, response *msg.MsgClientProposalResponse) error {
	rs.node.Mu.Lock()
	defer rs.node.Mu.Unlock()
	log.Printf("received ClientProposal rpc, command: %v", string(req.Command))
	if rs.node.CrrLeader == rs.node.NodeID {
		log.Printf("appending to log, command: %v", string(req.Command))
		rs.node.AppendToLog(rs.node.TermID, req.Command)
		response.Success = true
	} else {
		log.Printf("sending leaders info to client, leader: %v", rs.node.CrrLeader)
		response.Response = "Leader: " + (string)(rs.node.CrrLeader)
		response.Success = false
	}
	return nil
}
