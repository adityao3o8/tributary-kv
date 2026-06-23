package raft

import quorumpb "github.com/adityasingh/quorum/proto"

// raftLog is the in-memory replicated log.
//
// Indexing is 1-based: entries[0] is a sentinel with term 0 so that
// prevLogIndex/prevLogTerm arithmetic in AppendEntries needs no special-casing
// at the start of the log. The first real entry is at index 1.
//
// Phase 2 adds a snapshot base offset; for now the base index is always 0.
type raftLog struct {
	entries []*quorumpb.LogEntry
}

func newLog() *raftLog {
	return &raftLog{entries: []*quorumpb.LogEntry{{Term: 0}}}
}

// lastIndex returns the index of the last entry (0 if only the sentinel).
func (l *raftLog) lastIndex() uint64 {
	return uint64(len(l.entries) - 1)
}

// lastTerm returns the term of the last entry.
func (l *raftLog) lastTerm() uint64 {
	return l.entries[len(l.entries)-1].GetTerm()
}

// has reports whether index is present in the log.
func (l *raftLog) has(index uint64) bool {
	return index <= l.lastIndex()
}

// term returns the term of the entry at index. Index 0 has term 0.
func (l *raftLog) term(index uint64) uint64 {
	return l.entries[index].GetTerm()
}

// at returns the entry at index (index >= 1).
func (l *raftLog) at(index uint64) *quorumpb.LogEntry {
	return l.entries[index]
}

// from returns a copy of all entries at index..lastIndex (index >= 1).
func (l *raftLog) from(index uint64) []*quorumpb.LogEntry {
	src := l.entries[index:]
	out := make([]*quorumpb.LogEntry, len(src))
	copy(out, src)
	return out
}

// append adds entries to the end of the log.
func (l *raftLog) append(entries ...*quorumpb.LogEntry) {
	l.entries = append(l.entries, entries...)
}

// truncateFrom drops every entry at index and beyond (index >= 1).
func (l *raftLog) truncateFrom(index uint64) {
	l.entries = l.entries[:index]
}

// firstIndexOfTerm returns the first index whose entry has the given term,
// scanning from the start. Used for the AppendEntries fast-backup hint.
func (l *raftLog) firstIndexOfTerm(t uint64) uint64 {
	for i := uint64(1); i <= l.lastIndex(); i++ {
		if l.entries[i].GetTerm() == t {
			return i
		}
	}
	return 0
}

// lastIndexOfTerm returns the last index whose entry has the given term, or 0
// if no entry has it. Used by the leader to skip past a conflicting term.
func (l *raftLog) lastIndexOfTerm(t uint64) uint64 {
	for i := l.lastIndex(); i >= 1; i-- {
		if l.entries[i].GetTerm() == t {
			return i
		}
	}
	return 0
}
