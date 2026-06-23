// Package server implements the gRPC services exposed by a quorum node.
//
// Phase 0: KV handlers mutate the in-memory store directly, and Peer.Ping
// answers node-to-node health checks. There is no leader redirection or client
// request dedup yet (Phase 3).
package server

import (
	"context"

	"github.com/adityasingh/quorum/internal/store"
	quorumpb "github.com/adityasingh/quorum/proto"
)

// Server implements both the KV (client-facing) and Peer (node-to-node)
// gRPC services for a single node.
type Server struct {
	quorumpb.UnimplementedKVServer
	quorumpb.UnimplementedPeerServer

	id    string
	store *store.Store
}

// New returns a Server with the given node ID backed by store s.
func New(id string, s *store.Store) *Server {
	return &Server{id: id, store: s}
}

// Put stores a key/value pair.
func (s *Server) Put(_ context.Context, req *quorumpb.PutRequest) (*quorumpb.PutResponse, error) {
	s.store.Put(req.GetKey(), req.GetValue())
	return &quorumpb.PutResponse{}, nil
}

// Get returns the value for a key and whether it was found.
func (s *Server) Get(_ context.Context, req *quorumpb.GetRequest) (*quorumpb.GetResponse, error) {
	v, ok := s.store.Get(req.GetKey())
	return &quorumpb.GetResponse{Value: v, Found: ok}, nil
}

// Delete removes a key and reports whether it existed.
func (s *Server) Delete(_ context.Context, req *quorumpb.DeleteRequest) (*quorumpb.DeleteResponse, error) {
	existed := s.store.Delete(req.GetKey())
	return &quorumpb.DeleteResponse{Existed: existed}, nil
}

// Ping answers node-to-node health checks.
func (s *Server) Ping(_ context.Context, req *quorumpb.PingRequest) (*quorumpb.PingResponse, error) {
	return &quorumpb.PingResponse{From: s.id, Nonce: req.GetNonce()}, nil
}
