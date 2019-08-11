package paxosio

import (
	"context"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

// RequestHandler is the handler function type for handling received message
// batch. Received message batches should be passed to the request handler to
// have them processed by go-paxos.
type RequestHandler func(req paxospb.MessageBatch)

// ChunkSinkFactory is a factory function that returns a new IChunkSink
// instance. The returned IChunkSink will be used to accept future received
// snapshot chunks.
type ChunkSinkFactory func() IChunkSink

// IChunkSink is the interface of snapshot chunk sink. IChunkSink is used to
// accept received snapshot chunks.
type IChunkSink interface {
	// Close closes the sink instance and releases all resources held by it.
	Close()
	// AddChunk adds a new snapshot chunk to the snapshot chunk sink. All chunks
	// belong to the snapshot will be combined into the snapshot image and then
	// be passed to Dragonboat once all member chunks are received.

	// AddChunk(chunk paxospb.SnapshotChunk)

	// Tick moves forward the internal logic clock. It is suppose to be called
	// roughly every second.
	Tick()
}

// IConnection is the interface used by the Raft RPC module for sending Raft
// messages. Each IConnection works for a specified target nodehost instance,
// it is possible for a target to have multiple concurrent IConnection
// instances.
type IConnection interface {
	// Close closes the IConnection instance.
	Close()
	// SendMessageBatch sends the specified message batch to the target. It is
	// recommended to deliver the message batch to the target in order to enjoy
	// best possible performance, but out of order delivery is allowed at the
	// cost of reduced performance.
	SendMessageBatch(batch paxospb.MessageBatch) error
}

//IPaxosRPC ...
type IPaxosRPC interface {
	Name() string
	Start() error
	Stop()
	// GetConnection returns an IConnection instance responsible for
	// sending Raft messages to the specified target nodehost.
	GetConnection(ctx context.Context, target string) (IConnection, error)
}
