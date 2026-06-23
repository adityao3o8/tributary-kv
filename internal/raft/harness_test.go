package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adityasingh/quorum/internal/transport"
)

// cluster is an in-process Raft cluster wired over the controllable in-memory
// network, with each node's applied entries recorded for agreement checks. It
// is the seed of the multi-node test harness later phases build on.
type cluster struct {
	t     *testing.T
	net   *transport.Network
	ids   []string
	nodes map[string]*Node

	mu      sync.Mutex
	applied map[string][]ApplyMsg // per-node, in apply order
}

func testConfig() Config {
	return Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  40 * time.Millisecond,
		RPCTimeout:         60 * time.Millisecond,
	}
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		net:     transport.NewNetwork(),
		nodes:   make(map[string]*Node),
		applied: make(map[string][]ApplyMsg),
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		var peers []string
		for _, other := range c.ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		applyCh := make(chan ApplyMsg, 256)
		node := New(id, peers, c.net.Transport(id), NewInMemPersister(), applyCh, testConfig())
		c.nodes[id] = node
		c.net.Join(id, node)
		go c.drain(id, applyCh)
	}
	for _, id := range c.ids {
		c.nodes[id].Start()
	}
	t.Cleanup(c.stop)
	return c
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

func (c *cluster) drain(id string, ch chan ApplyMsg) {
	for m := range ch {
		c.mu.Lock()
		c.applied[id] = append(c.applied[id], m)
		c.mu.Unlock()
	}
}

// disconnect isolates a node from every other node (both directions).
func (c *cluster) disconnect(id string) {
	for _, other := range c.ids {
		if other != id {
			c.net.Partition(id, other)
		}
	}
}

// connect restores a node's links to every other node.
func (c *cluster) connect(id string) {
	for _, other := range c.ids {
		if other != id {
			c.net.Heal(id, other)
		}
	}
}

// setDropAll applies a drop probability to every directed link.
func (c *cluster) setDropAll(p float64) {
	for _, a := range c.ids {
		for _, b := range c.ids {
			if a != b {
				c.net.SetDropRate(a, b, p)
			}
		}
	}
}

// checkOneLeader waits for and returns the ID of the single leader. It fails if
// no leader or multiple leaders for the same term emerge within the deadline.
func (c *cluster) checkOneLeader() string {
	return c.checkOneLeaderAmong(c.ids)
}

// checkOneLeaderAmong is like checkOneLeader but only considers the given
// candidates. Use it in partition tests, where a stale isolated ex-leader must
// be excluded from the connected set.
func (c *cluster) checkOneLeaderAmong(candidates []string) string {
	c.t.Helper()
	for iters := 0; iters < 12; iters++ {
		time.Sleep(120 * time.Millisecond)
		leadersByTerm := make(map[uint64][]string)
		for _, id := range candidates {
			if term, isLeader := c.nodes[id].GetState(); isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], id)
			}
		}
		lastTerm := uint64(0)
		for term, leaders := range leadersByTerm {
			if len(leaders) > 1 {
				c.t.Fatalf("term %d has multiple leaders: %v", term, leaders)
			}
			if term > lastTerm {
				lastTerm = term
			}
		}
		if lastTerm > 0 && len(leadersByTerm[lastTerm]) == 1 {
			return leadersByTerm[lastTerm][0]
		}
	}
	c.t.Fatalf("no single leader elected")
	return ""
}

// nCommitted reports how many nodes have applied an entry at the given index
// and the command there, failing if they disagree.
func (c *cluster) nCommitted(index uint64) (int, string) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	cmd := ""
	for _, id := range c.ids {
		entries := c.applied[id]
		if uint64(len(entries)) < index {
			continue
		}
		got := string(entries[index-1].Command)
		if count > 0 && got != cmd {
			c.t.Fatalf("applied mismatch at index %d: %q vs %q", index, cmd, got)
		}
		count++
		cmd = got
	}
	return count, cmd
}

// submitTo submits a command to the named node.
func (c *cluster) submitTo(id, cmd string) (uint64, uint64, bool) {
	return c.nodes[id].Submit([]byte(cmd))
}

// one submits cmd, expecting it to commit on at least expected nodes at a fresh
// index, retrying against whichever node is leader. Returns the commit index.
func (c *cluster) one(cmd string, expected int) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		// Find a node that accepts the command as leader.
		var index uint64
		accepted := false
		for _, id := range c.ids {
			if idx, _, ok := c.submitTo(id, cmd); ok {
				index = idx
				accepted = true
				break
			}
		}
		if accepted {
			// Wait for it to commit on the expected number of nodes with the
			// right command.
			commitDeadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(commitDeadline) {
				if cnt, got := c.nCommitted(index); cnt >= expected && got == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("command %q never committed on %d nodes", cmd, expected)
	return 0
}
