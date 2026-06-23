package raft

import (
	"sync"

	quorumpb "github.com/adityasingh/quorum/proto"
)

// PersistentState is the subset of Raft state that MUST survive a crash
// (Raft §5, Figure 2): currentTerm, votedFor, and the log. It must be written
// to stable storage before a node responds to any RPC that depends on it.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string // "" means none
	Log         []*quorumpb.LogEntry
}

// Persister abstracts stable storage for Raft state. Phase 1 ships an in-memory
// implementation for tests; Phase 2 adds a disk-backed write-ahead log behind
// this same interface.
type Persister interface {
	Save(PersistentState)
	Load() (PersistentState, bool)
}

// InMemPersister is a volatile Persister. It still models the "persist before
// respond" discipline (Save is synchronous) so tests exercise the same code
// path, but it does not survive process restart.
type InMemPersister struct {
	mu    sync.Mutex
	state PersistentState
	saved bool
}

// NewInMemPersister returns an empty in-memory persister.
func NewInMemPersister() *InMemPersister {
	return &InMemPersister{}
}

// Save stores a deep-enough copy of the state.
func (p *InMemPersister) Save(s PersistentState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	logCopy := make([]*quorumpb.LogEntry, len(s.Log))
	copy(logCopy, s.Log)
	s.Log = logCopy
	p.state = s
	p.saved = true
}

// Load returns the saved state and whether anything was ever saved.
func (p *InMemPersister) Load() (PersistentState, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.saved {
		return PersistentState{}, false
	}
	logCopy := make([]*quorumpb.LogEntry, len(p.state.Log))
	copy(logCopy, p.state.Log)
	return PersistentState{
		CurrentTerm: p.state.CurrentTerm,
		VotedFor:    p.state.VotedFor,
		Log:         logCopy,
	}, true
}
