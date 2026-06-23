package transport

import (
	"context"
	"fmt"
	"sync"

	quorumpb "github.com/adityasingh/quorum/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport is the real network implementation: it dials peers over gRPC
// using a static ID -> address map. In Phase 0 a single node has no peers, so
// this exists mainly to keep the interface honest; Phase 1 exercises it for
// real Raft RPCs.
type GRPCTransport struct {
	self  string
	addrs map[string]string

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewGRPCTransport builds a transport for the local node `self`, where `addrs`
// maps every node ID (including self) to its host:port.
func NewGRPCTransport(self string, addrs map[string]string) *GRPCTransport {
	cp := make(map[string]string, len(addrs))
	for k, v := range addrs {
		cp[k] = v
	}
	return &GRPCTransport{self: self, addrs: cp, conns: make(map[string]*grpc.ClientConn)}
}

// Peer returns a handle that dials the target node lazily and reuses the
// connection on subsequent calls.
func (t *GRPCTransport) Peer(id string) (Peer, error) {
	addr, ok := t.addrs[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownPeer, id)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	conn, ok := t.conns[id]
	if !ok {
		c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("transport: dial %s (%s): %w", id, addr, err)
		}
		conn = c
		t.conns[id] = conn
	}
	return &grpcPeer{client: quorumpb.NewPeerClient(conn)}, nil
}

// Close tears down all open peer connections.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, conn := range t.conns {
		_ = conn.Close()
		delete(t.conns, id)
	}
	return nil
}

type grpcPeer struct {
	client quorumpb.PeerClient
}

func (p *grpcPeer) Ping(ctx context.Context, req *quorumpb.PingRequest) (*quorumpb.PingResponse, error) {
	return p.client.Ping(ctx, req)
}

func (p *grpcPeer) RequestVote(ctx context.Context, req *quorumpb.RequestVoteRequest) (*quorumpb.RequestVoteResponse, error) {
	return p.client.RequestVote(ctx, req)
}

func (p *grpcPeer) AppendEntries(ctx context.Context, req *quorumpb.AppendEntriesRequest) (*quorumpb.AppendEntriesResponse, error) {
	return p.client.AppendEntries(ctx, req)
}
