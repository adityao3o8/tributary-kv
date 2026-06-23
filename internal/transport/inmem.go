package transport

import (
	"context"
	"math/rand"
	"sync"
	"time"

	quorumpb "github.com/adityasingh/quorum/proto"
)

// Network is an in-process switch that routes RPCs between nodes registered
// with Join. It is the seed of the Phase 6 chaos harness: every link can be
// partitioned, made to drop messages probabilistically, or delayed.
//
// Faults are configured per directed link (from -> to); the Partition/Heal
// helpers operate on both directions for convenience.
type Network struct {
	mu          sync.Mutex
	nodes       map[string]PeerServer
	partitioned map[link]bool
	dropRate    map[link]float64
	delay       map[link]time.Duration
	rng         *rand.Rand
}

type link struct{ from, to string }

// NewNetwork returns an empty in-memory network.
func NewNetwork() *Network {
	return &Network{
		nodes:       make(map[string]PeerServer),
		partitioned: make(map[link]bool),
		dropRate:    make(map[link]float64),
		delay:       make(map[link]time.Duration),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Join registers a node's inbound handler under the given ID.
func (n *Network) Join(id string, h PeerServer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = h
}

// Transport returns a Transport scoped to the node with the given ID. RPCs sent
// through it have that node as their source for fault-injection purposes.
func (n *Network) Transport(id string) Transport {
	return &inmemTransport{net: n, self: id}
}

// Partition cuts the link between a and b in both directions.
func (n *Network) Partition(a, b string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned[link{a, b}] = true
	n.partitioned[link{b, a}] = true
}

// Heal restores the link between a and b in both directions.
func (n *Network) Heal(a, b string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.partitioned, link{a, b})
	delete(n.partitioned, link{b, a})
}

// HealAll clears every partition.
func (n *Network) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned = make(map[link]bool)
}

// SetDropRate sets the probability [0,1] that a message from -> to is dropped.
func (n *Network) SetDropRate(from, to string, p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate[link{from, to}] = p
}

// SetDelay sets an artificial latency on the directed link from -> to.
func (n *Network) SetDelay(from, to string, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay[link{from, to}] = d
}

// deliver applies fault injection for from -> to and, if the message survives,
// returns the destination handler. The delay (if any) is applied before
// returning so it counts toward the caller's context deadline.
func (n *Network) deliver(ctx context.Context, from, to string) (PeerServer, error) {
	n.mu.Lock()
	h, known := n.nodes[to]
	partitioned := n.partitioned[link{from, to}]
	drop := n.dropRate[link{from, to}]
	delay := n.delay[link{from, to}]
	dropped := drop > 0 && n.rng.Float64() < drop
	n.mu.Unlock()

	if !known {
		return nil, ErrUnknownPeer
	}
	if partitioned {
		return nil, ErrUnreachable
	}
	if dropped {
		return nil, ErrDropped
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h, nil
}

type inmemTransport struct {
	net  *Network
	self string
}

func (t *inmemTransport) Peer(id string) (Peer, error) {
	return &inmemPeer{net: t.net, from: t.self, to: id}, nil
}

type inmemPeer struct {
	net      *Network
	from, to string
}

func (p *inmemPeer) Ping(ctx context.Context, req *quorumpb.PingRequest) (*quorumpb.PingResponse, error) {
	h, err := p.net.deliver(ctx, p.from, p.to)
	if err != nil {
		return nil, err
	}
	return h.Ping(ctx, req)
}

func (p *inmemPeer) RequestVote(ctx context.Context, req *quorumpb.RequestVoteRequest) (*quorumpb.RequestVoteResponse, error) {
	h, err := p.net.deliver(ctx, p.from, p.to)
	if err != nil {
		return nil, err
	}
	return h.RequestVote(ctx, req)
}

func (p *inmemPeer) AppendEntries(ctx context.Context, req *quorumpb.AppendEntriesRequest) (*quorumpb.AppendEntriesResponse, error) {
	h, err := p.net.deliver(ctx, p.from, p.to)
	if err != nil {
		return nil, err
	}
	return h.AppendEntries(ctx, req)
}
