# Quorum — A Distributed, Strongly-Consistent Key-Value Store

> A from-scratch Raft-based KV store with sharding, linearizable reads, and a
> fault-injection (Jepsen-lite) test harness that proves correctness under
> network partitions and node failures.

---

## 1. Why this project exists (the résumé thesis)

Every existing project in the portfolio puts an API in front of a model or a
database. None demonstrate **core distributed systems**: consensus, replication,
partitioning, fault tolerance. This is the exact muscle probed in SDE system-design
rounds. This project fills that gap and adds **Go** to the stack.

**Target résumé bullets (earn these by building):**
- Implemented the Raft consensus protocol from scratch in Go (leader election,
  log replication, snapshots, membership changes).
- Built a sharded, horizontally-scalable KV store with linearizable reads via
  ReadIndex and exactly-once client semantics.
- Verified linearizability under network partitions, message loss, and node
  crashes using a fault-injection harness + Porcupine linearizability checker.
- Benchmarked X ops/sec across an N-node cluster with p99 latency of Y ms.

---

## 2. Architecture overview

```
            ┌─────────── client (quorumctl / Go client lib) ───────────┐
            │  routes by key → shard → Raft group leader               │
            └──────────────────────────┬───────────────────────────────┘
                                        │ gRPC
         ┌──────────────────────────────┼──────────────────────────────┐
         │              Shard Controller (its own Raft group)           │
         │      owns the shard→group config map; rebalances shards       │
         └──────────────────────────────┬──────────────────────────────┘
                                        │
        ┌───────────────────────────────┼───────────────────────────────┐
        ▼                                ▼                                ▼
 ┌─────────────┐                 ┌─────────────┐                  ┌─────────────┐
 │ Raft group 0│                 │ Raft group 1│                  │ Raft group N│
 │ (shards a,d)│                 │ (shards b,e)│                  │ (shards c,f)│
 │ ┌─┐ ┌─┐ ┌─┐ │                 │ ┌─┐ ┌─┐ ┌─┐ │                  │ ┌─┐ ┌─┐ ┌─┐ │
 │ │L│ │F│ │F│ │                 │ │L│ │F│ │F│ │                  │ │L│ │F│ │F│ │
 │ └─┘ └─┘ └─┘ │                 │ └─┘ └─┘ └─┘ │                  │ └─┘ └─┘ └─┘ │
 └─────────────┘                 └─────────────┘                  └─────────────┘
   each node = Raft peer + WAL + snapshot + KV state machine
```

**Layering (bottom → top):**
1. **Transport** — pluggable network. Real impl = gRPC. Test impl = in-memory
   switch you can partition/drop/delay. *This abstraction is what makes chaos
   testing possible — design it early.*
2. **Raft core** — consensus over a replicated log. Knows nothing about KV.
3. **State machine** — applies committed log entries to an in-memory KV map.
4. **Server** — gRPC API, leader redirection, dedup of retried client requests.
5. **Sharding** — consistent hashing + a Raft-replicated shard controller.
6. **Client** — Go library + CLI that routes keys to the right group/leader.

---

## 3. Repository layout

```
quorum/
├── cmd/
│   ├── quorumd/         # server daemon: starts a node
│   └── quorumctl/       # CLI client
├── internal/
│   ├── raft/            # consensus core (the heart)
│   │   ├── raft.go      #   node struct, main loop, tick
│   │   ├── state.go     #   role, term, votedFor, persistent state
│   │   ├── election.go  #   RequestVote logic
│   │   ├── replicate.go #   AppendEntries logic + commit advancement
│   │   ├── log.go       #   in-memory log: append, truncate, term-at-index
│   │   ├── snapshot.go  #   compaction + InstallSnapshot (Phase 2)
│   │   └── persist.go   #   durable currentTerm/votedFor/log
│   ├── store/           # KV state machine (Put/Get/Delete/CAS)
│   ├── server/          # gRPC server, client request dedup, leader redirect
│   ├── shard/           # shard controller + key routing (Phase 4)
│   └── transport/       # network interface: gRPC impl + in-mem test impl
├── pkg/client/          # public Go client library
├── proto/               # protobuf service + message defs
├── test/
│   ├── cluster/         # in-process N-node harness
│   └── chaos/           # fault injection + Porcupine checks (Phase 6)
├── deploy/docker-compose.yml
├── go.mod
└── README.md
```

Why `internal/`: prevents other modules importing your guts — signals you know
Go conventions. `pkg/client` is the one thing meant to be imported.

---

## 4. The month-plus roadmap

Estimates assume a few focused hours/day around classes. Don't skip the test
harness in Phase 1 — it pays for itself ten times over.

### Phase 0 — Foundations (2 days)
- `go mod init`, repo skeleton, gRPC + protobuf toolchain working.
- `transport` interface defined with both gRPC and in-memory implementations.
- A trivial end-to-end "hello": one node, Put/Get against a plain map, no Raft.
- **Milestone:** `quorumctl put k v` then `quorumctl get k` works on a single node.

### Phase 1 — Raft core (7–10 days) ← the heart
- Leader election: terms, election timeouts (randomized), RequestVote.
- Log replication: AppendEntries, the consistency check, commitIndex advance.
- Apply committed entries to the state machine.
- Persistence of (currentTerm, votedFor, log) — but disk snapshots come Phase 2.
- **In-process cluster harness** with a controllable network from day one.
- **Milestone:** 3-node cluster elects a leader; survives leader kill and
  re-elects; replicated log stays consistent across nodes. Tests pass with
  random message reordering/drops.

### Phase 2 — Persistence & snapshots (3–4 days)
- Write-ahead log to disk; restart replays it.
- Snapshotting / log compaction; `InstallSnapshot` RPC for lagging followers.
- **Milestone:** kill a node mid-run, restart it, it catches up via snapshot +
  log and rejoins cleanly.

### Phase 3 — Linearizable KV layer (3–4 days)
- KV ops as Raft commands; leader-only writes with follower redirection.
- **Linearizable reads via ReadIndex** (don't just read the leader's map —
  handle the stale-leader problem explicitly; this is a favorite interview gotcha).
- Client request IDs → dedup table so retried requests apply once (exactly-once
  from the client's view).
- **Milestone:** concurrent clients, no lost/duplicated writes, reads never go
  back in time.

### Phase 4 — Sharding (7 days)
- Consistent hashing maps keys → shards.
- **Shard controller** = its own small Raft group holding the shard→group config.
- Multiple Raft groups, one set of replicas per group.
- Client routes each key to the owning group's leader.
- Shard migration when config changes (hand off ownership without losing writes).
- **Milestone:** add a group, watch shards rebalance, no data lost during migration.

### Phase 5 — Membership changes (2–3 days)
- Add/remove nodes from a Raft group via single-server config changes (simpler
  and safer than full joint consensus for v1).
- **Milestone:** grow a 3-node group to 5 and shrink back, live, no downtime.

### Phase 6 — Observability + Chaos (4–5 days) ← the differentiator
- Prometheus metrics (term, commit index, apply lag, leader changes, op latency).
- OpenTelemetry traces; Grafana dashboard.
- **Jepsen-lite harness**: inject partitions, message loss, clock skew, crashes
  while a workload runs; record the operation history; feed it to **Porcupine**
  (Go linearizability checker) to *prove* the history is linearizable.
- **Milestone:** a reproducible test that partitions the network mid-write and
  Porcupine confirms linearizability held.

### Phase 7 — Polish & demo (3–4 days)
- `docker-compose` to spin a local cluster.
- README with architecture diagram, the chaos results, and benchmarks.
- Short demo GIF/asciinema: kill the leader, watch it heal, data intact.

---

## 5. Reference material (use these, don't reinvent)

- **Raft paper** — Ongaro & Ousterhout, "In Search of an Understandable
  Consensus Algorithm." Figure 2 is your spec; implement it literally.
- **raft.github.io** — the visualization; great for building intuition.
- **MIT 6.5840 (formerly 6.824)** labs — this project mirrors their KV + shardkv
  arc. Use for structure/tests-to-pass *as inspiration*; write your own code.
- **Porcupine** (github.com/anishathalye/porcupine) — linearizability checker.
- **The Secret Lives of Data** — animated Raft walkthrough.

---

## 6. Design decisions to lock early (and the gotchas)

- **One goroutine owns Raft state, or a single mutex guards it.** Don't let
  RPCs mutate term/log concurrently — this is where most from-scratch Raft
  implementations get subtle, irreproducible bugs.
- **Persist before you respond.** Update currentTerm/votedFor/log on disk
  *before* replying to RequestVote/AppendEntries, or you'll violate safety
  after a crash.
- **Election timeout must be randomized** (e.g. 150–300ms) and clearly larger
  than the heartbeat interval, or you get perpetual split votes.
- **The AppendEntries consistency check (prevLogIndex/prevLogTerm) is the whole
  ballgame** for log matching — get this exactly right before moving on.
- **ReadIndex, not "read from leader's map"** — a partitioned old leader still
  thinks it's leader. Linearizable reads need a quorum confirmation.
- **Transport abstraction first.** If chaos testing is bolted on later it's
  painful; if the network is an interface from day one it's trivial.
