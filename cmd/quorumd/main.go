// Command quorumd runs a single quorum node.
//
// Phase 0: serves the KV API over gRPC against an in-memory store, plus the
// node-to-node Peer service. No Raft yet.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/adityasingh/quorum/internal/server"
	"github.com/adityasingh/quorum/internal/store"
	quorumpb "github.com/adityasingh/quorum/proto"
	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", "node-1", "node id")
	listen := flag.String("listen", ":7000", "gRPC listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("quorumd: listen %s: %v", *listen, err)
	}

	srv := server.New(*id, store.New())
	gs := grpc.NewServer()
	quorumpb.RegisterKVServer(gs, srv)
	quorumpb.RegisterPeerServer(gs, srv)

	go func() {
		log.Printf("quorumd %s listening on %s", *id, *listen)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("quorumd: serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("quorumd %s shutting down", *id)
	gs.GracefulStop()
}
