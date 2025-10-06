package node

import (
	"context"
	"math/rand"
	"net/rpc"
	"raft/config"
	raftlog "raft/log"
	"raft/msg"
	"raft/statemachine"
	"raft/types"
	"strings"
	"sync"
	"time"

	"log"
)

type Node struct {
	NodeID      int
	NodeAddr    string
	Peers       []string
	PeerClients []*rpc.Client
	NodeState   NodeState

	// raft state
	TermID       types.TermID
	LastVotedFor int
	Log          raftlog.Log
	CommitIdx    int
	LastApplied  int
	NextIdx      []types.MsgID
	MatchIdx     []types.MsgID
	CrrLeader    int

	StateMachine statemachine.StateMachine

	LeaderPingTimestamp time.Time

	BecomeFollowerChan        chan bool
	BecomeLeaderChan          chan bool
	StopLeaderHealthCheckChan chan bool
	StopSendEntriesChan       chan bool

	LeaderCtx         context.Context
	CancelLeaderCtx   context.CancelFunc
	FollowerCtx       context.Context
	CancelFollowerCtx context.CancelFunc

	Mu sync.Mutex
}

func NewNode(nodeID int, nodeAddr string, peers []string) *Node {
	Log := raftlog.NewInMemLog()

	becomeFollowerChan := make(chan bool)
	becomeLeaderChan := make(chan bool)
	stopSendEntriesChan := make(chan bool)
	stopLeaderHealthChan := make(chan bool)

	stateMachine := statemachine.NewSimpleStateMachine()

	return &Node{
		NodeID:                    nodeID,
		NodeAddr:                  nodeAddr,
		Peers:                     peers,
		NodeState:                 FOLLOWER,
		TermID:                    0,
		LastVotedFor:              -1,
		Log:                       Log,
		CommitIdx:                 -1,
		LastApplied:               -1,
		StateMachine:              stateMachine,
		LeaderPingTimestamp:       time.Now(),
		BecomeFollowerChan:        becomeFollowerChan,
		BecomeLeaderChan:          becomeLeaderChan,
		StopSendEntriesChan:       stopSendEntriesChan,
		StopLeaderHealthCheckChan: stopLeaderHealthChan,
	}
}

func (n *Node) InitializePeerConn() {
	var peerClients []*rpc.Client
	peerClients = make([]*rpc.Client, 0)
	for _, peer := range n.Peers {
		peerHost := strings.Split(peer, ":")[0]
		peerPort := strings.Split(peer, ":")[1]

		var client *rpc.Client
		var err error
		for {
			client, err = rpc.Dial("tcp", peerHost+":"+peerPort)
			if err != nil {
				log.Printf("error init client: %v\n", err)
			} else {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		log.Printf("connected to peer: %v", peer)

		peerClients = append(peerClients, client)
	}
	n.PeerClients = peerClients
	n.NextIdx = make([]types.MsgID, len(n.Peers))
	n.MatchIdx = make([]types.MsgID, len(n.Peers))
	for i := range n.MatchIdx {
		n.MatchIdx[i] = -1
	}
}

func (n *Node) LatestRecord() (types.TermID, types.MsgID) {
	return n.Log.LatestEntry()
}

func (n *Node) Run(ctx context.Context) {
	n.InitializePeerConn()
	go n.LogStatus(ctx)
	n.FollowerCtx, n.CancelFollowerCtx = context.WithCancel(context.Background())
	go n.LeaderHealth(n.FollowerCtx)
	go n.StateMachineApply(ctx)
	<-ctx.Done()
}

func (n *Node) AppendToLog(termId types.TermID, cmd []byte) {
	n.Log.AppendEntry(termId, cmd)
}

// func (n *Node) NodeStateTransitioner() {
// 	for {
// 		select {
// 		case <-n.BecomeFollowerChan:
// 			n.StopLeaderHealthCheckChan <- true
// 			go n.SendEntries()
// 		case <-n.BecomeLeaderChan:
// 			n.StopSendEntriesChan <- true
// 			go n.LeaderHealth()
// 		}
// 	}
// }

func (n *Node) BecomeLeader() {
	if n.NodeState == LEADER {
		return
	}
	if n.CancelFollowerCtx != nil {
		n.CancelFollowerCtx()
	}
	n.NodeState = LEADER
	_, msgId := n.Log.LatestEntry()
	for i := range n.Peers {
		n.NextIdx[i] = msgId + 1
		n.MatchIdx[i] = 0
	}
	n.CrrLeader = n.NodeID
	n.LeaderCtx, n.CancelLeaderCtx = context.WithCancel(context.Background())
	go n.SendEntries(n.LeaderCtx)
	go n.LogCommitter(n.LeaderCtx)
}

func (n *Node) BecomeFollower() {
	if n.NodeState == FOLLOWER {
		return
	}
	if n.CancelLeaderCtx != nil {
		n.CancelLeaderCtx()
	}
	n.NodeState = FOLLOWER
	n.FollowerCtx, n.CancelFollowerCtx = context.WithCancel(context.Background())
	go n.LeaderHealth(n.FollowerCtx)
}

func (n *Node) LogCommitter(leaderCtx context.Context) {
	ticker := time.NewTicker(config.UPDATE_COMMIT_INTERVAL * time.Second)
	log.Printf("[Node: %v] log commiter goroutine started with commit idx: %v", n.NodeID, n.CommitIdx)
	for {
		select {
		case <-ticker.C:
			log.Printf("[Node: %v] log committer ticked!!", n.NodeID)
			n.Mu.Lock()
			nextCommitIdx := n.CommitIdx + 1
			cnt := 1
			for i := range n.MatchIdx {
				if nextCommitIdx <= int(n.MatchIdx[i]) {
					cnt++
				}
			}
			if 2*cnt > len(n.Peers)+1 {
				n.CommitIdx = nextCommitIdx
			}
			log.Printf("next commit idx: %v, cnt: %v, commit idx: %v", nextCommitIdx, cnt, n.CommitIdx)
			n.Mu.Unlock()
		case <-leaderCtx.Done():
			return
		}
	}

}

func (n *Node) StateMachineApply(ctx context.Context) {
	ticker := time.NewTicker(config.STATE_MACHINE_APPLY_INTERVAL * time.Second)
	log.Printf("[Node: %v] state machine apply goroutine started with commit idx: %v, last applied: %v", n.NodeID, n.CommitIdx, n.LastApplied)
	for {
		select {
		case <-ticker.C:
			n.Mu.Lock()
			log.Printf("[Node: %v] statemachine apply ticked!!", n.NodeID)
			for n.CommitIdx > n.LastApplied {
				n.LastApplied++
				n.StateMachine.Apply(n.Log.GetEntry(types.MsgID(n.LastApplied)).Command)
				log.Printf("[Node: %v] idx %v applied to state machine", n.NodeID, n.LastApplied-1)
			}
			n.Mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (n *Node) SendEntries(leaderCtx context.Context) {
	log.Printf("[Node: %v] starting to send AppendEntries", n.NodeID)

	for i := range n.Peers {
		go n.SendEntriesToPeer(leaderCtx, i)
	}
}

func (n *Node) SendEntriesToPeer(leaderCtx context.Context, peerId int) {
	ticker := time.NewTicker(config.LEADER_PING_INTERVAL * time.Second)
	log.Printf("[Node: %v] starting to send AppendEntries to peer %v", n.NodeID, n.Peers[peerId])

	var msgAppendEntriesResponse msg.MsgAppendEntriesResponse
	var response *rpc.Call
	for {
		select {
		case <-ticker.C:
			log.Printf("ticker for AppendEntries to peer: %v", n.Peers[peerId])
			n.Mu.Lock()
			// log.Printf("[1] lock acquired ticker for AppendEntries to peer: %v", n.Peers[peerId])
			lastEntryTermId, lastEntryMsgId := n.Log.LatestEntry()
			msgAppendEntries := msg.MsgAppendEntries{
				TermID:          n.TermID,
				LeaderID:        n.NodeID,
				LastEntryTermID: lastEntryTermId,
				LastEntryMsgID:  lastEntryMsgId,
				Entries:         nil,
				LeaderCommit:    n.CommitIdx,
			}
			// log.Printf("[2] lock acquired ticker for AppendEntries to peer: %v", n.Peers[peerId])
			if n.NextIdx[peerId] <= lastEntryMsgId {
				// log.Printf("[3] lock acquired ticker for AppendEntries to peer: %v", n.Peers[peerId])
				entries := make([]raftlog.Entry, 0)
				for i := n.NextIdx[peerId]; i <= lastEntryMsgId; i++ {
					entries = append(entries, n.Log.GetEntry(i))
				}
				msgAppendEntries.Entries = entries
				if n.NextIdx[peerId] > 0 {
					lastEntry := n.Log.GetEntry(n.NextIdx[peerId] - 1)
					msgAppendEntries.LastEntryTermID = lastEntry.TermID
					msgAppendEntries.LastEntryMsgID = lastEntry.MsgID
				} else {
					msgAppendEntries.LastEntryTermID = -1
					msgAppendEntries.LastEntryMsgID = -1
				}
				// log.Printf("[4] lock acquired ticker for AppendEntries to peer: %v", n.Peers[peerId])
			}
			// log.Printf("[5] lock acquired ticker for AppendEntries to peer: %v", n.Peers[peerId])
			log.Printf("sending AppendEntries with %v entries to peer: %v", len(msgAppendEntries.Entries), n.Peers[peerId])
			response = n.PeerClients[peerId].Go("RaftRPCService.AppendEntries", &msgAppendEntries, &msgAppendEntriesResponse, nil)
			n.Mu.Unlock()

			timeout := time.NewTimer(config.LEADER_PING_INTERVAL * time.Second / 10)
			select {
			case <-response.Done:
				log.Printf("got response of AppendEntries from peer: %v", n.Peers[peerId])
				n.Mu.Lock()
				if msgAppendEntriesResponse.Success {
					n.NextIdx[peerId] = lastEntryMsgId + 1
					n.MatchIdx[peerId] = lastEntryMsgId
					log.Printf("successfully appended entries to peer: %v, with next Idx: %v, match Idx: %v", n.Peers[peerId], n.NextIdx[peerId], n.MatchIdx[peerId])
				} else {
					log.Printf("failed to append entries to peer: %v", n.Peers[peerId])
					if n.NextIdx[peerId] > 0 {
						n.NextIdx[peerId]--
					}
					if msgAppendEntriesResponse.TermID > n.TermID {
						n.BecomeFollower()
					}
				}
				n.Mu.Unlock()
			case <-timeout.C:
				log.Printf("response timeout of AppendEntries from peer: %v", n.Peers[peerId])
				continue
			}

		case <-leaderCtx.Done():
			return
		}
	}
}

func (n *Node) LeaderHealth(followerCtx context.Context) {
	ticker := time.NewTicker(config.LEADER_TIMEOUT * time.Second)

	for {
		select {
		case <-ticker.C:
			if time.Since(n.LeaderPingTimestamp) > config.LEADER_TIMEOUT*time.Second {
				log.Printf("leader heartbeats not received, starting election")
				n.StartElection()
			} else {
				log.Printf("leader alive, Term: %v, last heartbeat timestamp: %v", n.TermID, n.LeaderPingTimestamp)
			}

		case <-followerCtx.Done():
			return
		}
	}
}

func (n *Node) StartElection() {
	var electionDone chan bool
	electionDone = make(chan bool)
	var ctx context.Context
	var cancel context.CancelFunc
	round := 0
	rand.Seed(time.Now().UnixNano())
	for {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration((config.ELECTION_TIMEOUT+rand.Float64()*config.ELECTION_TIMEOUT_JITTER)*float64(time.Millisecond)))
		go n.ElectionRound(ctx, electionDone)

		<-electionDone
		log.Printf("election round %v finished", round)
		cancel()
		time.Sleep(time.Duration((rand.Float64() * config.ELECTION_TIMEOUT_JITTER) * float64(time.Millisecond)))
		n.Mu.Lock()
		if n.NodeState == LEADER || n.NodeState == FOLLOWER {
			log.Printf("election ended, node state: %v, term: %v", n.NodeState, n.TermID)
			n.Mu.Unlock()
			return
		}
		n.Mu.Unlock()
		round++
	}
}

func (n *Node) ElectionRound(ctx context.Context, electionDone chan bool) {
	n.Mu.Lock()
	n.NodeState = CANDIDATE
	n.TermID++
	n.LastVotedFor = n.NodeID
	latestTermID, latestMsgID := n.Log.LatestEntry()
	msgRequestVote := msg.MsgRequestVote{
		CandidateID:        n.NodeID,
		TermID:             n.TermID,
		LatestRecordTermID: latestTermID,
		LatestRecordMsgID:  latestMsgID,
	}
	n.Mu.Unlock()
	var msgResquestVoteResponses []msg.MsgRequestVoteResponse
	var responses []*rpc.Call

	msgResquestVoteResponses = make([]msg.MsgRequestVoteResponse, len(n.Peers))
	responses = make([]*rpc.Call, 0)

	// log.Printf("Length of the peers: %v, peer: %v", len(n.Peers), n.Peers[0])

	for idx, client := range n.PeerClients {
		resp := client.Go("RaftRPCService.RequestVote", &msgRequestVote, &msgResquestVoteResponses[idx], nil)
		responses = append(responses, resp)
	}

	voteCount := 1 // self vote
	for idx, response := range responses {
		select {
		case _ = <-response.Done:
			if msgResquestVoteResponses[idx].Vote {
				voteCount++
				log.Printf("got the vote from peer, vote count: %v for term: %v", voteCount, n.TermID)
			} else {
				log.Printf("peer rejected vote")
			}
			n.Mu.Lock()
			if 2*voteCount > len(n.Peers)+1 {
				n.BecomeLeader()
				n.Mu.Unlock()
				electionDone <- true
				return
			}
			n.Mu.Unlock()

		case <-ctx.Done():
			n.Mu.Lock()
			log.Printf("election got timed out, TermID: %v, Node State: %v", n.TermID, n.NodeState)
			n.Mu.Unlock()
			electionDone <- true
			return
		}
	}
	electionDone <- true
}

// func (n *Node) StepDownFromLeader() {
// 	if n.NodeState == LEADER {
// 		go n.LeaderHealth()
// 		n.PingFollowersChan <- false
// 	}
// }

func (n *Node) LogStatus(ctx context.Context) {
	stateMap := map[NodeState]string{
		0: "FOLLOWER",
		1: "CANDIDATE",
		2: "LEADER",
	}
	ticker := time.NewTicker(config.STATUS_LOGGING_TIME_INTERVAL * time.Second)

	for {
		select {
		case <-ticker.C:
			n.Mu.Lock()
			log.Printf("[Node ID: %v] Node State: %v Current Term: %v Log: %v State Machine State: %v\n", n.NodeID, stateMap[n.NodeState], n.TermID, n.Log.Print(), n.StateMachine.GetState())
			n.Mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}
