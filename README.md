# Quorum

A from-scratch, Raft-based distributed key-value store in Go, with sharding,
linearizable reads, and a fault-injection harness that proves correctness under
partitions and crashes.

This repo is built in phases — see [docs/DESIGN.md](docs/DESIGN.md) for the full
architecture and 7-phase roadmap.

## Status: Phase 0 — Foundations ✅

What works today:
- Repo skeleton + gRPC/protobuf toolchain.
- A **transport abstraction** with two implementations:
  - `grpc` — real node-to-node networking.
  - `inmem` — a controllable in-process switch with **partition / drop / delay**
    knobs (the seed of the Phase 6 chaos harness), proven by unit tests.
- A single node serving a KV API (`Put` / `Get` / `Delete`) over an in-memory
  map. No Raft yet — that's Phase 1.

## Prerequisites

- Go 1.24+
- [`buf`](https://buf.build) and `protoc-gen-go` / `protoc-gen-go-grpc`
  (only needed if you regenerate protobufs):

```sh
brew install go buf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## Quickstart

```sh
make gen      # regenerate protobuf code (optional; checked in)
make build    # build ./bin/quorumd and ./bin/quorumctl
make test     # run unit + integration tests

# Run a node, then talk to it:
./bin/quorumd --listen :7000 &
./bin/quorumctl --addr :7000 put hello world
./bin/quorumctl --addr :7000 get hello       # -> world
./bin/quorumctl --addr :7000 delete hello
./bin/quorumctl --addr :7000 get hello       # -> (not found)
```

## Layout

| Path                  | What                                             |
|-----------------------|--------------------------------------------------|
| `cmd/quorumd`         | node daemon                                       |
| `cmd/quorumctl`       | CLI client                                        |
| `internal/store`      | KV state machine (plain map for now)              |
| `internal/server`     | gRPC KV + Peer service handlers                   |
| `internal/transport`  | network interface: gRPC + in-mem fault-injecting  |
| `pkg/client`          | public Go client library                          |
| `proto`               | protobuf service + message definitions            |
| `test/cluster`        | in-process node harness                           |
