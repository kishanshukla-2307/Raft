package msg

import (
	"raft/replicatedlog"
	"raft/types"
)

type MsgAppendEntries struct {
	TermID          types.TermID
	LeaderID        int
	LastEntryTermID types.TermID
	LastEntryMsgID  types.MsgID
	Entries         []replicatedlog.Entry
}

type MsgAppendEntriesResponse struct {
	TermID  types.TermID
	Success bool
}
