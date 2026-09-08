// Package relayclient 提供 worker/CS 共用的 gRPC relay 连接池。
// 包只依赖 proto 与 gRPC，避免 deliver/exchange 相互 import 形成依赖环。
package relayclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"alfred.brave.com/event"
	"alfred.brave.com/joker/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var log = event.Log

// Client 按 CS 地址复用连接；CallTimeout 保护任意 unary 调用不被半开连接永久阻塞。
type Client struct {
	mu          sync.Mutex
	conns       map[string]*grpc.ClientConn
	CallTimeout time.Duration
}

func New() *Client {
	return &Client{conns: make(map[string]*grpc.ClientConn), CallTimeout: 5 * time.Second}
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 20 * time.Second, Timeout: 5 * time.Second, PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("relay dial %s: %w", addr, err)
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *Client) evict(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		delete(c.conns, addr)
		if err := conn.Close(); err != nil {
			log.Warnf("relay close evicted conn %s: %v", addr, err)
		}
	}
}

func (c *Client) call(ctx context.Context, addr string, fn func(jokerproto.RelayClient, context.Context) error) error {
	conn, err := c.conn(addr)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.CallTimeout)
	defer cancel()
	if err := fn(jokerproto.NewRelayClient(conn), callCtx); err != nil {
		if status.Code(err) == codes.Unavailable {
			c.evict(addr)
		}
		return err
	}
	return nil
}

func (c *Client) RelayMessage(ctx context.Context, addr string, req *jokerproto.RelayMessageRequest) (*jokerproto.RelayMessageResponse, error) {
	var response *jokerproto.RelayMessageResponse
	err := c.call(ctx, addr, func(client jokerproto.RelayClient, callCtx context.Context) error {
		var err error
		response, err = client.RelayMessage(callCtx, req)
		return err
	})
	return response, err
}

func (c *Client) BatchRelayMessages(ctx context.Context, addr string, req *jokerproto.BatchRelayMessagesRequest) (*jokerproto.BatchRelayMessagesResponse, error) {
	var response *jokerproto.BatchRelayMessagesResponse
	err := c.call(ctx, addr, func(client jokerproto.RelayClient, callCtx context.Context) error {
		var err error
		response, err = client.BatchRelayMessages(callCtx, req)
		return err
	})
	return response, err
}

func (c *Client) BatchRelayAcks(ctx context.Context, addr string, req *jokerproto.BatchRelayAcksRequest) (*jokerproto.BatchRelayAcksResponse, error) {
	var response *jokerproto.BatchRelayAcksResponse
	err := c.call(ctx, addr, func(client jokerproto.RelayClient, callCtx context.Context) error {
		var err error
		response, err = client.BatchRelayAcks(callCtx, req)
		return err
	})
	return response, err
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, conn := range c.conns {
		if err := conn.Close(); err != nil {
			log.Warnf("relay close conn %s: %v", addr, err)
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)
}
