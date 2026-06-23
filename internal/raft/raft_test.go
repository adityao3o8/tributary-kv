package raft

import (
	"fmt"
	"testing"
	"time"
)

// A 3-node cluster elects exactly one leader and keeps it while heartbeats flow.
func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)

	leader := c.checkOneLeader()

	term1, _ := c.nodes[leader].GetState()
	time.Sleep(500 * time.Millisecond)
	// Leadership should be stable: still exactly one leader, same or no new term
	// churn from spurious elections.
	leader2 := c.checkOneLeader()
	term2, _ := c.nodes[leader2].GetState()
	if leader2 != leader {
		t.Fatalf("leader changed without cause: %s -> %s", leader, leader2)
	}
	if term2 != term1 {
		t.Fatalf("term churned without cause: %d -> %d", term1, term2)
	}
}

// Killing the leader (network isolation) triggers a re-election; restoring it
// leaves the cluster with a single leader again.
func TestReElection(t *testing.T) {
	c := newCluster(t, 3)

	leader1 := c.checkOneLeader()

	// Isolate the leader; the remaining two must elect a new one.
	c.disconnect(leader1)
	var connected []string
	for _, id := range c.ids {
		if id != leader1 {
			connected = append(connected, id)
		}
	}
	leader2 := c.checkOneLeaderAmong(connected)
	if leader2 == leader1 {
		t.Fatalf("isolated leader %s should not still be the cluster leader", leader1)
	}

	// Reconnect the old leader; it must step down on seeing the higher term,
	// leaving the whole cluster with a single leader again.
	c.connect(leader1)
	leader3 := c.checkOneLeader()
	if _, isLeader := c.nodes[leader1].GetState(); isLeader && leader3 != leader1 {
		t.Fatalf("old leader %s failed to step down", leader1)
	}
}

// A minority partition cannot elect a leader or commit. (Note: the isolated
// old leader still *believes* it is leader — basic Raft has no check-quorum
// step-down until leader leases arrive in Phase 3 — so we assert about the
// connected minority node specifically, not all nodes.)
func TestMinorityCannotElect(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()

	// Disconnect the leader and one follower, leaving a single connected node.
	var follower, lone string
	for _, id := range c.ids {
		if id != leader {
			if follower == "" {
				follower = id
			} else {
				lone = id
			}
		}
	}
	c.disconnect(leader)
	c.disconnect(follower)

	time.Sleep(800 * time.Millisecond)

	// The lone connected node keeps timing out and re-running elections but can
	// never win a majority, so it must not become leader.
	if _, isLeader := c.nodes[lone].GetState(); isLeader {
		t.Fatalf("lone minority node %s should not be able to win an election", lone)
	}
	// And it cannot accept writes (not leader), so nothing new commits.
	if _, _, ok := c.submitTo(lone, "nope"); ok {
		t.Fatalf("lone minority node %s accepted a write without quorum", lone)
	}
}

// Commands submitted to the leader are replicated and applied identically on
// every node, in order.
func TestLogReplication(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader()

	for i := 1; i <= 5; i++ {
		cmd := fmt.Sprintf("set-x-%d", i)
		index := c.one(cmd, 3)
		if index != uint64(i) {
			t.Fatalf("command %q committed at index %d, want %d", cmd, index, i)
		}
	}
}

// Progress continues with one follower disconnected (majority intact); when it
// reconnects it catches up to the same committed log.
func TestAgreementWithFollowerDown(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.checkOneLeader()

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	c.disconnect(follower)
	// Majority (2 of 3) still commits.
	c.one("a", 2)
	c.one("b", 2)

	// Reconnect; the lagging follower should catch up to index 2.
	c.connect(follower)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cnt, _ := c.nCommitted(2); cnt == 3 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("reconnected follower never caught up")
}

// Agreement is reached even when the network drops a fraction of messages;
// heartbeats and retries eventually replicate everything.
func TestAgreementUnderDrops(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader()
	c.setDropAll(0.2)

	for i := 1; i <= 5; i++ {
		c.one(fmt.Sprintf("v%d", i), 2)
	}
}
