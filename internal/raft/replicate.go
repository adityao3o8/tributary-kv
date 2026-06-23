package raft

import (
	"context"
	"time"

	quorumpb "github.com/adityasingh/quorum/proto"
)

// leaderLoop sends heartbeats / replicates at the heartbeat interval for as
// long as this node remains leader for the given term.
func (n *Node) leaderLoop(term uint64) {
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()

	n.broadcastAppendEntries() // assert leadership immediately
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			n.mu.Lock()
			stillLeader := n.role == leader && n.currentTerm == term
			n.mu.Unlock()
			if !stillLeader {
				return
			}
			n.broadcastAppendEntries()
		}
	}
}

// broadcastAppendEntries fans out replication RPCs to every peer.
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != leader {
		n.mu.Unlock()
		return
	}
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateTo(peer)
	}
}

// replicateTo sends one AppendEntries to peer and processes the reply,
// adjusting nextIndex/matchIndex and advancing the commit index (Raft §5.3).
func (n *Node) replicateTo(peer string) {
	n.mu.Lock()
	if n.role != leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	nextIdx := n.nextIndex[peer]
	if nextIdx < 1 {
		nextIdx = 1
	}
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.log.term(prevLogIndex)
	var entries []*quorumpb.LogEntry
	if n.log.has(nextIdx) {
		entries = n.log.from(nextIdx)
	}
	req := &quorumpb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	handle, err := n.transport.Peer(peer)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	resp, err := handle.AppendEntries(ctx, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	// Drop stale replies.
	if n.currentTerm != term || n.role != leader {
		return
	}
	if resp.GetTerm() > n.currentTerm {
		n.becomeFollowerLocked(resp.GetTerm())
		n.persistLocked()
		return
	}

	if resp.GetSuccess() {
		match := prevLogIndex + uint64(len(entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitLocked()
		return
	}

	// Failure: back nextIndex up using the follower's conflict hint, skipping a
	// whole term at a time rather than one entry per round trip.
	switch {
	case resp.GetConflictTerm() == 0:
		// Follower's log is shorter than prevLogIndex.
		n.nextIndex[peer] = resp.GetConflictIndex()
	default:
		if last := n.log.lastIndexOfTerm(resp.GetConflictTerm()); last != 0 {
			n.nextIndex[peer] = last + 1
		} else {
			n.nextIndex[peer] = resp.GetConflictIndex()
		}
	}
	if n.nextIndex[peer] < 1 {
		n.nextIndex[peer] = 1
	}
}

// advanceCommitLocked moves commitIndex forward to the highest index replicated
// on a majority, but only for entries from the current term (Raft §5.4.2).
func (n *Node) advanceCommitLocked() {
	for idx := n.log.lastIndex(); idx > n.commitIndex; idx-- {
		if n.log.term(idx) != n.currentTerm {
			continue
		}
		count := 1 // self
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.applyCond.Broadcast()
			return
		}
	}
}

// AppendEntries handles replication / heartbeats from a leader (Raft §5.3).
func (n *Node) AppendEntries(_ context.Context, req *quorumpb.AppendEntriesRequest) (*quorumpb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &quorumpb.AppendEntriesResponse{}

	// Reject older terms.
	if req.GetTerm() < n.currentTerm {
		resp.Term = n.currentTerm
		resp.Success = false
		return resp, nil
	}
	// Recognize the leader: adopt its term (if newer) and revert to follower.
	if req.GetTerm() > n.currentTerm {
		n.becomeFollowerLocked(req.GetTerm())
	}
	n.role = follower
	n.resetElectionTimerLocked()
	resp.Term = n.currentTerm

	// Consistency check: our log must contain prevLogIndex with prevLogTerm.
	if req.GetPrevLogIndex() > n.log.lastIndex() {
		// We're too short; tell the leader where our log ends.
		resp.Success = false
		resp.ConflictTerm = 0
		resp.ConflictIndex = n.log.lastIndex() + 1
		n.persistLocked()
		return resp, nil
	}
	if n.log.term(req.GetPrevLogIndex()) != req.GetPrevLogTerm() {
		conflictTerm := n.log.term(req.GetPrevLogIndex())
		resp.Success = false
		resp.ConflictTerm = conflictTerm
		resp.ConflictIndex = n.log.firstIndexOfTerm(conflictTerm)
		// Drop the conflicting suffix.
		n.log.truncateFrom(req.GetPrevLogIndex())
		n.persistLocked()
		return resp, nil
	}

	// Append new entries, overwriting any that conflict by term (§5.3).
	for i, e := range req.GetEntries() {
		idx := req.GetPrevLogIndex() + 1 + uint64(i)
		if n.log.has(idx) {
			if n.log.term(idx) == e.GetTerm() {
				continue // already present and matching
			}
			n.log.truncateFrom(idx)
		}
		n.log.append(e)
	}
	n.persistLocked()

	// Advance commit to min(leaderCommit, index of last new entry).
	if req.GetLeaderCommit() > n.commitIndex {
		newCommit := req.GetLeaderCommit()
		if last := n.log.lastIndex(); newCommit > last {
			newCommit = last
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.applyCond.Broadcast()
		}
	}

	resp.Success = true
	return resp, nil
}
