package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	quorumpb "github.com/adityasingh/quorum/proto"
)

// pinger is a trivial PeerServer that echoes the nonce back, tagged with its
// own ID.
type pinger struct{ id string }

func (p *pinger) Ping(_ context.Context, req *quorumpb.PingRequest) (*quorumpb.PingResponse, error) {
	return &quorumpb.PingResponse{From: p.id, Nonce: req.GetNonce()}, nil
}

// The remaining PeerServer methods are unused by these transport tests.
func (p *pinger) RequestVote(context.Context, *quorumpb.RequestVoteRequest) (*quorumpb.RequestVoteResponse, error) {
	return &quorumpb.RequestVoteResponse{}, nil
}

func (p *pinger) AppendEntries(context.Context, *quorumpb.AppendEntriesRequest) (*quorumpb.AppendEntriesResponse, error) {
	return &quorumpb.AppendEntriesResponse{}, nil
}

func TestInmemPingDelivers(t *testing.T) {
	net := NewNetwork()
	net.Join("a", &pinger{id: "a"})
	net.Join("b", &pinger{id: "b"})

	peer, err := net.Transport("a").Peer("b")
	if err != nil {
		t.Fatalf("Peer: %v", err)
	}
	resp, err := peer.Ping(context.Background(), &quorumpb.PingRequest{From: "a", Nonce: 42})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if resp.GetFrom() != "b" || resp.GetNonce() != 42 {
		t.Fatalf("Ping resp = (%q, %d), want (b, 42)", resp.GetFrom(), resp.GetNonce())
	}
}

func TestInmemUnknownPeer(t *testing.T) {
	net := NewNetwork()
	net.Join("a", &pinger{id: "a"})

	peer, _ := net.Transport("a").Peer("ghost")
	_, err := peer.Ping(context.Background(), &quorumpb.PingRequest{From: "a"})
	if !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("Ping to unknown peer err = %v, want ErrUnknownPeer", err)
	}
}

func TestInmemPartition(t *testing.T) {
	net := NewNetwork()
	net.Join("a", &pinger{id: "a"})
	net.Join("b", &pinger{id: "b"})

	peerA, _ := net.Transport("a").Peer("b")
	peerB, _ := net.Transport("b").Peer("a")

	net.Partition("a", "b")

	if _, err := peerA.Ping(context.Background(), &quorumpb.PingRequest{From: "a"}); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("partitioned a->b err = %v, want ErrUnreachable", err)
	}
	// Partition is symmetric.
	if _, err := peerB.Ping(context.Background(), &quorumpb.PingRequest{From: "b"}); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("partitioned b->a err = %v, want ErrUnreachable", err)
	}

	net.Heal("a", "b")
	if _, err := peerA.Ping(context.Background(), &quorumpb.PingRequest{From: "a"}); err != nil {
		t.Fatalf("after heal a->b err = %v, want nil", err)
	}
}

func TestInmemDropRate(t *testing.T) {
	net := NewNetwork()
	net.Join("a", &pinger{id: "a"})
	net.Join("b", &pinger{id: "b"})
	net.SetDropRate("a", "b", 1.0)

	peer, _ := net.Transport("a").Peer("b")
	for i := 0; i < 20; i++ {
		if _, err := peer.Ping(context.Background(), &quorumpb.PingRequest{From: "a"}); !errors.Is(err, ErrDropped) {
			t.Fatalf("with DropRate=1.0, err = %v, want ErrDropped", err)
		}
	}

	// Drop rate is directional: b -> a is unaffected.
	peerB, _ := net.Transport("b").Peer("a")
	if _, err := peerB.Ping(context.Background(), &quorumpb.PingRequest{From: "b"}); err != nil {
		t.Fatalf("reverse link err = %v, want nil", err)
	}
}

func TestInmemDelayRespectsContext(t *testing.T) {
	net := NewNetwork()
	net.Join("a", &pinger{id: "a"})
	net.Join("b", &pinger{id: "b"})
	net.SetDelay("a", "b", 200*time.Millisecond)

	peer, _ := net.Transport("a").Peer("b")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if _, err := peer.Ping(ctx, &quorumpb.PingRequest{From: "a"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delayed call err = %v, want DeadlineExceeded", err)
	}
}
