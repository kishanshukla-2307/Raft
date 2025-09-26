package node

import (
	"context"
	"math/rand"
	"net/rpc"
	"raft/config"
	"raft/msg"
	"raft/replicatedlog"
	"raft/types"
	"strings"
	"sync"
	"time"

	"log"
)

type Node struct {
	NodeID                int
	NodeAddr              string
	Peers                 []string
	PeerClients           []*rpc.Client
	NodeState             NodeState
	TermID                types.TermID
	Log                   replicatedlog.Log
	LeaderPingTimestamp   time.Time
	LastVotedFor          int
	CheckLeaderHealthChan chan bool
	PingFollowersChan     chan bool

	Mu sync.Mutex
}

func NewNode(nodeID int, nodeAddr string, peers []string) *Node {
	Log := replicatedlog.NewInMemLog()

	var pingFollowers chan bool
	pingFollowers = make(chan bool)
	var checkLeaderHealth chan bool
	checkLeaderHealth = make(chan bool)

	return &Node{
		NodeID:                nodeID,
		NodeAddr:              nodeAddr,
		Peers:                 peers,
		NodeState:             FOLLOWER,
		TermID:                0,
		LastVotedFor:          -1,
		Log:                   Log,
		LeaderPingTimestamp:   time.Now(),
		PingFollowersChan:     pingFollowers,
		CheckLeaderHealthChan: checkLeaderHealth,
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
}

func (n *Node) LatestRecord() (types.TermID, types.MsgID) {
	return n.Log.LatestEntry()
}

func (n *Node) Run(ctx context.Context) {
	n.InitializePeerConn()
	var done chan bool
	go n.LogStatus(done)
	go n.LeaderHealth()
	<-ctx.Done()
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
			log.Printf("election ended, node state: %v", n.NodeState)
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
				log.Printf("got the vote from peer, vote count: %v", voteCount)
			} else {
				log.Printf("peer rejected vote")
			}
			n.Mu.Lock()
			if 2*voteCount > len(n.Peers)+1 {
				n.NodeState = LEADER
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

func (n *Node) LeaderHealth() {
	ticker := time.NewTicker(config.LEADER_TIMEOUT * time.Second)

	for {
		select {
		case <-ticker.C:
			if time.Since(n.LeaderPingTimestamp) > config.LEADER_TIMEOUT*time.Second {
				log.Printf("leader heartbeats not received, starting election")
				n.StartElection()
				if n.NodeState == LEADER {
					go n.PingFollowers()
					return
				}
			} else {
				log.Printf("leader alive, last heartbeat timestamp: %v", n.LeaderPingTimestamp)
			}

		case check := <-n.CheckLeaderHealthChan:
			if !check {
				return
			}
		}
	}
}

func (n *Node) PingFollowers() {
	ticker := time.NewTicker(config.LEADER_PING_INTERVAL * time.Second)
	log.Printf("leader starting to send AppendEntries")

	for {
		select {
		case <-ticker.C:
			n.Mu.Lock()
			msgAppendEntries := msg.MsgAppendEntries{
				TermID:          n.TermID,
				LeaderID:        n.NodeID,
				LastEntryTermID: 0,
				LastEntryMsgID:  0,
				Entries:         nil,
			}
			n.Mu.Unlock()
			var msgAppendEntriesResponse []msg.MsgAppendEntriesResponse
			var responses []*rpc.Call

			msgAppendEntriesResponse = make([]msg.MsgAppendEntriesResponse, len(n.Peers))
			responses = make([]*rpc.Call, 0)

			for idx, client := range n.PeerClients {
				log.Printf("sending AppendEntries to node id: %v", n.Peers[idx])
				response := client.Go("RaftRPCService.AppendEntries", &msgAppendEntries, &msgAppendEntriesResponse[idx], nil)
				responses = append(responses, response)
			}
			for idx, response := range responses {
				resp := <-response.Done
				if resp.Error != nil {
					log.Printf("error in AppendEntries rpc call to peer id: %v, err: %v", idx, resp.Error)
				}
			}
		case ping := <-n.PingFollowersChan:
			if !ping {
				return
			}
		}
	}
}

func (n *Node) StepDownFromLeader() {
	if n.NodeState == LEADER {
		go n.LeaderHealth()
		n.PingFollowersChan <- false
	}
}

func (n *Node) LogStatus(done chan bool) {
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
			log.Printf("[Node ID: %v] Node State: %v Current Term: %v\n", n.NodeID, stateMap[n.NodeState], n.TermID)
			n.Mu.Unlock()
		case <-done:
			return
		}
	}
}
