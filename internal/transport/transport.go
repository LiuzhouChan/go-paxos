package transport

import (
	"sync"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	circuit "github.com/rubyist/circuitbreaker"
)

const (
	rpcMaxMsgSize = settings.MaxMessageSize
)

var (
	lazyFreeCycle = settings.Soft.LazyFreeCycle
)

var (
	plog = logger.GetLogger("transport")
)

// INodeAddressResolver converts the (cluster id, node id( tuple to network
// address
type INodeAddressResolver interface {
	Resolve(uint64, uint64) (string, string, error)
	ReverseResolve(string) []paxosio.NodeInfo
	AddRemoteAddress(uint64, uint64, string)
}

// IPaxosMessageHandler is the interface required to handle incoming paxos
// requests.
type IPaxosMessageHandler interface {
	HandleMessageBatch(batch paxospb.MessageBatch)
	// HandleUnreachable(groupID uint64, nodeID uint64)
	// HandleSnapshotStatus(groupID uint64, nodeID uint64, rejected bool)
	// HandleSnapshot(groupID uint64, nodeID uint64, from uint64)
}

// ITransport is the interface of the transport layer used for exchanging
// Paxos messages.
type ITransport interface {
	Name() string
	// SetUnmanagedDeploymentID()
	// SetDeploymentID(uint64)
	SetMessageHandler(IPaxosMessageHandler)
	RemoveMessageHandler()
	ASyncSend(paxospb.PaxosMsg) bool
	// ASyncSendSnapshot(paxospb.PaxosMsg) bool
	Stop()
}

// SendMessageBatchFunc is a func type that is used to determine whether the
// specified message batch should be sent. This func is used in test only.
type SendMessageBatchFunc func(paxospb.MessageBatch) (paxospb.MessageBatch, bool)

// Transport is the transport layer for delivering paxos messages and snapshots.
type Transport struct {
	mu struct {
		sync.Mutex
		queues   map[string]chan paxospb.PaxosMsg
		breakers map[string]*circuit.Breaker
	}
}
