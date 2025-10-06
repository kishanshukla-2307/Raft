package log

import (
	"raft/types"
)

type Log interface {
	AppendEntry(types.TermID, []byte)
	GetEntry(types.MsgID) Entry
	LatestEntry() (types.TermID, types.MsgID)
	GetLength() int
	DiscardEntries(types.MsgID)
	Print() string
}

type Entry struct {
	TermID  types.TermID
	MsgID   types.MsgID
	Command []byte
}

func (e Entry) Equal(other *Entry) bool {
	if e.TermID != other.TermID || e.MsgID != other.MsgID || len(e.Command) != len(other.Command) {
		return false
	}
	for i := range e.Command {
		if e.Command[i] != other.Command[i] {
			return false
		}
	}
	return true
}
