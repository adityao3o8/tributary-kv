package raft

import (
	"context"

	quorumpb "github.com/adityasingh/quorum/proto"
)

// startElection transitions to candidate, votes for itself, and solicits votes
// from peers in parallel (Raft §5.2).
func (n *Node) startElection() {
	n.mu.Lock()
	n.role = candidate
	n.currentTerm++
	n.votedFor = n.id
	n.persistLocked()
	n.resetElectionTimerLocked()

	term := n.currentTerm
	lastLogIndex := n.log.lastIndex()
	lastLogTerm := n.log.lastTerm()
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	votes := 1 // vote for self
	for _, peer := range peers {
		go func(peer string) {
			req := &quorumpb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			handle, err := n.transport.Peer(peer)
			if err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
			defer cancel()
			resp, err := handle.RequestVote(ctx, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			// Ignore stale replies (term moved on, or we're no longer running
			// this election).
			if n.currentTerm != term || n.role != candidate {
				return
			}
			if resp.GetTerm() > n.currentTerm {
				n.becomeFollowerLocked(resp.GetTerm())
				n.persistLocked()
				return
			}
			if resp.GetVoteGranted() {
				votes++
				if votes >= n.majority() {
					n.becomeLeaderLocked()
				}
			}
		}(peer)
	}
}

// becomeLeaderLocked initializes leader state and starts replication. Caller
// must hold n.mu.
func (n *Node) becomeLeaderLocked() {
	if n.role != candidate {
		return
	}
	n.role = leader
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.log.lastIndex() + 1
		n.matchIndex[peer] = 0
	}
	go n.leaderLoop(n.currentTerm)
}

// RequestVote handles an incoming vote request (Raft §5.2, §5.4.1).
func (n *Node) RequestVote(_ context.Context, req *quorumpb.RequestVoteRequest) (*quorumpb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &quorumpb.RequestVoteResponse{}

	// Reject older terms outright.
	if req.GetTerm() < n.currentTerm {
		resp.Term = n.currentTerm
		resp.VoteGranted = false
		return resp, nil
	}
	// A newer term forces us to step down (and clears votedFor).
	if req.GetTerm() > n.currentTerm {
		n.becomeFollowerLocked(req.GetTerm())
	}

	resp.Term = n.currentTerm
	upToDate := n.candidateUpToDateLocked(req.GetLastLogIndex(), req.GetLastLogTerm())
	if (n.votedFor == "" || n.votedFor == req.GetCandidateId()) && upToDate {
		n.votedFor = req.GetCandidateId()
		resp.VoteGranted = true
		n.resetElectionTimerLocked() // granting a vote defers our own election
	}
	n.persistLocked()
	return resp, nil
}

// candidateUpToDateLocked implements the §5.4.1 log-up-to-date check: the
// candidate's log must be at least as up to date as ours.
func (n *Node) candidateUpToDateLocked(lastIndex, lastTerm uint64) bool {
	myTerm := n.log.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= n.log.lastIndex()
}
