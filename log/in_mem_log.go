package log

import "raft/types"

var (
	_ Log = &InMemLog{}
)

type InMemLog struct {
	entries     []Entry
	commitIndex int
}

func NewInMemLog() *InMemLog {
	entries := make([]Entry, 0)
	return &InMemLog{
		entries: entries,
	}
}

func (l *InMemLog) AppendEntries(entries []Entry) {
	for _, entry := range entries {
		l.entries = append(l.entries, entry)
	}
}

func (l *InMemLog) DiscardUncommitedEntries() {
	l.entries = l.entries[:l.commitIndex]
}

func (l *InMemLog) LatestEntry() (types.TermID, types.MsgID) {
	if len(l.entries) == 0 {
		return -1, -1
	}
	latest_entry := l.entries[len(l.entries)-1]
	return latest_entry.TermID, latest_entry.MsgID
}
