// Package store holds the key-value state machine.
//
// Phase 0: a plain in-memory map guarded by a mutex, mutated directly by the
// gRPC server. There is no Raft yet.
//
// Forward-looking (Phase 3): this becomes the state machine that applies
// committed Raft log entries via an Apply(cmd) method. The surface is kept
// deliberately small so that refactor stays clean.
package store

import "sync"

// Store is a concurrency-safe in-memory key-value map.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Put sets key to value, overwriting any existing value.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get returns the value for key and whether it was present.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Delete removes key and reports whether it existed.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}
