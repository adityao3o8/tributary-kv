// Package transport is the pluggable network layer for node-to-node
// communication. It is defined as an interface from day one so that the chaos
// test harness (partitions, drops, delays) can swap a controllable in-memory
// network in place of real gRPC without any consensus code knowing.
//
// Phase 0 carries a single RPC, Ping, which is enough to prove the abstraction
// and its fault-injection knobs work. Phase 1 extends Peer/PeerServer with the
// Raft RPCs (RequestVote, AppendEntries).
package transport

import (
	"context"
	"errors"

	quorumpb "github.com/adityasingh/quorum/proto"
)

var (
	// ErrUnknownPeer is returned when no node with the requested ID exists.
	ErrUnknownPeer = errors.New("transport: unknown peer")
	// ErrUnreachable is returned when the link to a peer is partitioned.
	ErrUnreachable = errors.New("transport: peer unreachable (partitioned)")
	// ErrDropped is returned when a message is dropped by fault injection.
	ErrDropped = errors.New("transport: message dropped")
)

// PeerServer is the inbound side: a node implements this to handle RPCs from
// other nodes. The Raft node satisfies it.
type PeerServer interface {
	Ping(context.Context, *quorumpb.PingRequest) (*quorumpb.PingResponse, error)
	RequestVote(context.Context, *quorumpb.RequestVoteRequest) (*quorumpb.RequestVoteResponse, error)
	AppendEntries(context.Context, *quorumpb.AppendEntriesRequest) (*quorumpb.AppendEntriesResponse, error)
}

// Peer is the outbound side: a handle used to send RPCs to one remote node.
type Peer interface {
	Ping(context.Context, *quorumpb.PingRequest) (*quorumpb.PingResponse, error)
	RequestVote(context.Context, *quorumpb.RequestVoteRequest) (*quorumpb.RequestVoteResponse, error)
	AppendEntries(context.Context, *quorumpb.AppendEntriesRequest) (*quorumpb.AppendEntriesResponse, error)
}

// Transport is a node's view of the network: it hands out Peer handles to
// reach other nodes by ID. Each Transport is bound to a single local node.
type Transport interface {
	// Peer returns a handle for sending RPCs to the node with the given ID.
	Peer(id string) (Peer, error)
}
