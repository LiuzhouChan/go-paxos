package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

var (
	// NOOPPaxosName is the module name for the NOOP transport module.
	NOOPPaxosName = "noop-test-transport"
	// ErrRequestedToFail is the error used to indicate that the error is
	// requested.
	ErrRequestedToFail = errors.New("requested to returned error")
)

type noopRequest struct {
	mu   sync.Mutex
	fail bool
}

func (r *noopRequest) SetToFail(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = v
}

func (r *noopRequest) Fail() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail
}

type noopConnectRequest struct {
	mu   sync.Mutex
	fail bool
}

func (r *noopConnectRequest) SetToFail(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = v
}

func (r *noopConnectRequest) Fail() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail
}

// NOOPConnection is the connection used to exchange messages between node hosts.
type NOOPConnection struct {
	req *noopRequest
}

// Close closes the NOOPConnection instance.
func (c *NOOPConnection) Close() {
}

// SendMessageBatch return ErrRequestedToFail when requested.
func (c *NOOPConnection) SendMessageBatch(batch paxospb.MessageBatch) error {
	if c.req.Fail() {
		return ErrRequestedToFail
	}
	return nil
}

// NOOPTransport is a transport module for testing purposes. It does not
// actually has the ability to exchange messages or snapshots between
// nodehosts.
type NOOPTransport struct {
	connected  uint64
	tryConnect uint64
	req        *noopRequest
	connReq    *noopConnectRequest
}

// NewNOOPTransport creates a new NOOPTransport instance.
func NewNOOPTransport(nhConfig config.NodeHostConfig,
	requestHandler paxosio.RequestHandler) paxosio.IPaxosRPC {
	return &NOOPTransport{
		req:     &noopRequest{},
		connReq: &noopConnectRequest{},
	}
}

// Start starts the NOOPTransport instance.
func (g *NOOPTransport) Start() error {
	return nil
}

// Stop stops the NOOPTransport instance.
func (g *NOOPTransport) Stop() {
}

// GetConnection returns a connection.
func (g *NOOPTransport) GetConnection(ctx context.Context,
	target string) (paxosio.IConnection, error) {
	atomic.AddUint64(&g.tryConnect, 1)
	if g.connReq.Fail() {
		return nil, ErrRequestedToFail
	}
	atomic.AddUint64(&g.connected, 1)
	return &NOOPConnection{req: g.req}, nil
}

// Name returns the module name.
func (g *NOOPTransport) Name() string {
	return NOOPPaxosName
}
