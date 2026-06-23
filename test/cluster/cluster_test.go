// Package cluster_test boots a real node in-process and exercises the full
// client -> gRPC -> server -> store path. It is the seed of the multi-node
// harness later phases build on.
package cluster_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/adityasingh/quorum/internal/server"
	"github.com/adityasingh/quorum/internal/store"
	"github.com/adityasingh/quorum/pkg/client"
	quorumpb "github.com/adityasingh/quorum/proto"
	"google.golang.org/grpc"
)

// startNode brings up a gRPC node on an ephemeral port and returns its address
// plus a cleanup func.
func startNode(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := server.New("test-node", store.New())
	gs := grpc.NewServer()
	quorumpb.RegisterKVServer(gs, srv)
	quorumpb.RegisterPeerServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	return lis.Addr().String(), gs.GracefulStop
}

func TestSingleNodePutGetDelete(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Put(ctx, "hello", "world"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if v, found, err := c.Get(ctx, "hello"); err != nil || !found || v != "world" {
		t.Fatalf("get = (%q, %v, %v), want (world, true, nil)", v, found, err)
	}

	existed, err := c.Delete(ctx, "hello")
	if err != nil || !existed {
		t.Fatalf("delete = (%v, %v), want (true, nil)", existed, err)
	}
	if _, found, _ := c.Get(ctx, "hello"); found {
		t.Fatalf("get after delete: want not found")
	}
}
