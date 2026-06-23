// Package client is the public Go client library for talking to a quorum
// cluster. In Phase 0 it targets a single node; later phases add key->group
// routing and leader redirection.
package client

import (
	"context"
	"fmt"

	quorumpb "github.com/adityasingh/quorum/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a connection to a single quorum node's KV API.
type Client struct {
	conn *grpc.ClientConn
	kv   quorumpb.KVClient
}

// Dial connects to a quorum node at addr (host:port).
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	return &Client{conn: conn, kv: quorumpb.NewKVClient(conn)}, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Put stores key=value.
func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.kv.Put(ctx, &quorumpb.PutRequest{Key: key, Value: value})
	return err
}

// Get returns the value for key and whether it was found.
func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	resp, err := c.kv.Get(ctx, &quorumpb.GetRequest{Key: key})
	if err != nil {
		return "", false, err
	}
	return resp.GetValue(), resp.GetFound(), nil
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (existed bool, err error) {
	resp, err := c.kv.Delete(ctx, &quorumpb.DeleteRequest{Key: key})
	if err != nil {
		return false, err
	}
	return resp.GetExisted(), nil
}
