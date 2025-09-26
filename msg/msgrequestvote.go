package msg

import (
	"raft/types"
)

type MsgRequestVote struct {
	CandidateID        int
	TermID             types.TermID
	LatestRecordTermID types.TermID
	LatestRecordMsgID  types.MsgID
}

type MsgRequestVoteResponse struct {
	TermID types.TermID
	Vote   bool
}
