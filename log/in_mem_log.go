package log

import "raft/types"

var (
	_ Log = &InMemLog{}
)

type InMemLog struct {
	entries []Entry
}

func NewInMemLog() *InMemLog {
	entries := make([]Entry, 0)
	return &InMemLog{
		entries: entries,
	}
}

func (l *InMemLog) AppendEntry(termId types.TermID, cmd []byte) {
	msgId := (types.MsgID)(l.GetLength())
	entry := Entry{
		TermID:  termId,
		MsgID:   msgId,
		Command: cmd,
	}
	l.entries = append(l.entries, entry)
}

func (l *InMemLog) DiscardEntries(idx types.MsgID) {
	l.entries = l.entries[:idx]
}

func (l *InMemLog) LatestEntry() (types.TermID, types.MsgID) {
	if l.GetLength() == 0 {
		return -1, -1
	}
	latest_entry := l.entries[len(l.entries)-1]
	return latest_entry.TermID, latest_entry.MsgID
}

func (l *InMemLog) GetEntry(idx types.MsgID) Entry {
	return l.entries[idx]
}

func (l *InMemLog) GetLength() int {
	return len(l.entries)
}

func (l *InMemLog) Print() string {
	str := ""
	for i := range l.entries {
		str += string(l.entries[i].Command)
	}
	return str
}
