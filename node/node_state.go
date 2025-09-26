package node

type NodeState int

const (
	FOLLOWER NodeState = iota
	CANDIDATE
	LEADER
)
