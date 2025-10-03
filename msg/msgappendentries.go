package msg

import (
	"raft/log"
	"raft/types"
)

type MsgAppendEntries struct {
	TermID          types.TermID
	LeaderID        int
	LastEntryTermID types.TermID
	LastEntryMsgID  types.MsgID
	Entries         []log.Entry
}

type MsgAppendEntriesResponse struct {
	TermID  types.TermID
	Success bool
}
