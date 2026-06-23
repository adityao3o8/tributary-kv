// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm"), following Figure 2.
//
// The core knows nothing about key-value storage: it replicates an ordered log
// of opaque command blobs and reports committed entries on an apply channel.
// A single mutex (Node.mu) guards all mutable state; every RPC handler and
// background goroutine takes it, which keeps the state machine race-free and
// reproducible (see docs/DESIGN.md §6).
//
// Phase 1 scope: leader election, log replication, commit advancement, and
// volatile-but-correct persistence (via an in-memory Persister). Disk WAL and
// snapshots arrive in Phase 2.
package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/adityasingh/quorum/internal/transport"
	quorumpb "github.com/adityasingh/quorum/proto"
)

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	default:
		return "unknown"
	}
}

// ApplyMsg is delivered on the apply channel for each committed log entry, in
// index order. The state machine consumes these to advance its state.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Config tunes the timing of a node. Zero values fall back to sane defaults;
// tests shrink these to run elections quickly.
type Config struct {
	// ElectionTimeoutMin/Max bound the randomized election timeout. Must be
	// comfortably larger than HeartbeatInterval to avoid perpetual split votes.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	// RPCTimeout bounds a single outbound RPC.
	RPCTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 100 * time.Millisecond
	}
	return c
}

// Node is a single Raft peer.
type Node struct {
	mu sync.Mutex

	id        string
	peers     []string // other members, excluding self
	transport transport.Transport
	persister Persister
	cfg       Config

	// Persistent state (Figure 2) — saved before responding to RPCs.
	currentTerm uint64
	votedFor    string
	log         *raftLog

	// Volatile state.
	role        role
	commitIndex uint64
	lastApplied uint64

	// Leader-only volatile state, keyed by peer ID.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Election timing.
	electionResetAt time.Time
	electionTimeout time.Duration
	rng             *rand.Rand

	// Lifecycle.
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	ctx       context.Context
	cancel    context.CancelFunc
	stopped   bool
}

// New creates a node. peers lists the IDs of the other cluster members. It
// recovers any persisted state but does not start background goroutines until
// Start is called.
func New(id string, peers []string, tr transport.Transport, p Persister, applyCh chan ApplyMsg, cfg Config) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		id:         id,
		peers:      append([]string(nil), peers...),
		transport:  tr,
		persister:  p,
		cfg:        cfg.withDefaults(),
		log:        newLog(),
		role:       follower,
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(len(id)))),
		applyCh:    applyCh,
		ctx:        ctx,
		cancel:     cancel,
	}
	n.applyCond = sync.NewCond(&n.mu)

	if s, ok := p.Load(); ok {
		n.currentTerm = s.CurrentTerm
		n.votedFor = s.VotedFor
		if len(s.Log) > 0 {
			n.log.entries = append([]*quorumpb.LogEntry(nil), s.Log...)
		}
	}
	n.resetElectionTimerLocked()
	return n
}

// Start launches the election ticker and the apply loop.
func (n *Node) Start() {
	go n.ticker()
	go n.applier()
}

// Stop halts all background goroutines. It is idempotent.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	n.mu.Unlock()

	n.cancel()
	n.applyCond.Broadcast() // wake the applier so it can exit
}

// Submit proposes a command. If this node is the leader it appends the command
// to its log and begins replication, returning the entry's index and term. If
// not the leader it returns isLeader=false and the caller should retry on the
// leader.
func (n *Node) Submit(command []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != leader {
		return 0, n.currentTerm, false
	}
	n.log.append(&quorumpb.LogEntry{Term: n.currentTerm, Command: command})
	n.persistLocked()
	index = n.log.lastIndex()
	term = n.currentTerm
	go n.broadcastAppendEntries()
	return index, term, true
}

// GetState reports the current term and whether this node believes it is leader.
func (n *Node) GetState() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == leader
}

// ID returns the node's ID.
func (n *Node) ID() string { return n.id }

// --- internal helpers (caller must hold n.mu unless noted) ---

func (n *Node) majority() int { return (len(n.peers)+1)/2 + 1 }

func (n *Node) persistLocked() {
	n.persister.Save(PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log.entries,
	})
}

func (n *Node) resetElectionTimerLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.electionTimeout = n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(int64(span)+1))
	n.electionResetAt = time.Now()
}

// becomeFollowerLocked steps down to follower at the given term. If the term is
// new, votedFor is cleared. The caller is responsible for persisting.
func (n *Node) becomeFollowerLocked(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.role = follower
}

// ticker drives election timeouts. A follower or candidate that hasn't heard
// from a leader (or granted a vote) within its randomized timeout starts an
// election.
func (n *Node) ticker() {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			n.mu.Lock()
			if n.role != leader && time.Since(n.electionResetAt) >= n.electionTimeout {
				n.mu.Unlock()
				n.startElection()
				continue
			}
			n.mu.Unlock()
		}
	}
}

// applier delivers committed entries to applyCh in index order.
func (n *Node) applier() {
	for {
		n.mu.Lock()
		for n.commitIndex <= n.lastApplied && !n.stopped {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		var msgs []ApplyMsg
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			e := n.log.at(n.lastApplied)
			msgs = append(msgs, ApplyMsg{Index: n.lastApplied, Term: e.GetTerm(), Command: e.GetCommand()})
		}
		n.mu.Unlock()

		for _, m := range msgs {
			select {
			case n.applyCh <- m:
			case <-n.ctx.Done():
				return
			}
		}
	}
}

// Ping satisfies transport.PeerServer for node-to-node health checks.
func (n *Node) Ping(_ context.Context, req *quorumpb.PingRequest) (*quorumpb.PingResponse, error) {
	return &quorumpb.PingResponse{From: n.id, Nonce: req.GetNonce()}, nil
}
