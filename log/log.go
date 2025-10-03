package log

import (
	"raft/types"
)

type Log interface {
	AppendEntries([]Entry)
	DiscardUncommitedEntries()
	LatestEntry() (types.TermID, types.MsgID)
}

type Entry struct {
	TermID types.TermID
	MsgID  types.MsgID
	Data   []byte
}
