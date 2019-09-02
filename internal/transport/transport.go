package transport

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/internal/utils/logutil"
	"github.com/LiuzhouChan/go-paxos/internal/utils/syncutil"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

const (
	rpcMaxMsgSize = settings.MaxMessageSize
)

var (
	lazyFreeCycle     = settings.Soft.LazyFreeCycle
	rpcSendBufSize    = settings.Soft.SendQueueLength
	idleTimeout       = time.Minute
	dialTimeoutSecond = settings.Soft.GetConnectedTimeoutSecond
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
		queues map[string]chan paxospb.PaxosMsg
		// breakers map[string]*circuit.Breaker
	}
	serverCtx          *server.Context
	nhConfig           config.NodeHostConfig
	sourceAddress      string
	resolver           INodeAddressResolver
	stopper            *syncutil.Stopper
	paxosRPC           paxosio.IPaxosRPC
	handlerRemovedFlag uint32
	handler            atomic.Value
	ctx                context.Context
	cancel             context.CancelFunc
	// snapshotCount      int32
	// snapshotQueueMu    sync.Mutex
}

// NewTransport creates a new Transport object.
func NewTransport(nhConfig config.NodeHostConfig, ctx *server.Context,
	resolver INodeAddressResolver) *Transport {
	address := nhConfig.PaxosAddress
	stopper := syncutil.NewStopper()
	t := &Transport{
		nhConfig:      nhConfig,
		serverCtx:     ctx,
		sourceAddress: address,
		resolver:      resolver,
		stopper:       stopper,
	}
	paxosRPC := createTransportRPC(nhConfig, t.handleRequest)
	plog.Infof("Paxos RPC type:%s", paxosRPC.Name())
	t.paxosRPC = paxosRPC
	if err := t.paxosRPC.Start(); err != nil {
		panic(err)
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	t.mu.queues = make(map[string]chan paxospb.PaxosMsg)
	return t
}

// Name returns the type name of the transport module
func (t *Transport) Name() string {
	return t.paxosRPC.Name()
}

// GetPaxosRPC returns the paxos RPC instance
func (t *Transport) GetPaxosRPC() paxosio.IPaxosRPC {
	return t.paxosRPC
}

// Stop stops the Transport object
func (t *Transport) Stop() {
	t.cancel()
	t.stopper.Stop()
	t.paxosRPC.Stop()
}

// GetCircuitBreaker returns the circuit breaker used for the specified
// target node.
// func (t *Transport) GetCircuitBreaker(key string) *circuit.Breaker {
// 	t.mu.Lock()
// 	breaker, ok := t.mu.breakers[key]
// 	if !ok {
// 		breaker = netutil.NewBreaker()
// 		t.mu.breakers[key] = breaker
// 	}
// 	t.mu.Unlock()

// 	return breaker
// }

// SetMessageHandler sets the raft message handler.
func (t *Transport) SetMessageHandler(handler IPaxosMessageHandler) {
	v := t.handler.Load()
	if v != nil {
		panic("trying to set the grpctransport handler again")
	}
	t.handler.Store(handler)
}

// RemoveMessageHandler removes the raft message handler.
func (t *Transport) RemoveMessageHandler() {
	atomic.StoreUint32(&t.handlerRemovedFlag, 1)
}

func (t *Transport) handleRequest(req paxospb.MessageBatch) {
	if t.handlerRemoved() {
		return
	}
	handler := t.handler.Load()
	if handler == nil {
		return
	}
	addr := req.SourceAddress
	if len(addr) > 0 {
		for _, r := range req.Requests {
			if r.From != 0 {
				t.resolver.AddRemoteAddress(r.GroupId, r.From, addr)
			}
		}
	}
	handler.(IPaxosMessageHandler).HandleMessageBatch(req)
}

func (t *Transport) handlerRemoved() bool {
	return atomic.LoadUint32(&t.handlerRemovedFlag) == 1
}

// ASyncSend sends paxos msg using RPC
func (t *Transport) ASyncSend(req paxospb.PaxosMsg) bool {
	toNodeID := req.To
	groupID := req.GroupId
	from := req.From
	addr, key, err := t.resolver.Resolve(groupID, toNodeID)
	if err != nil {
		plog.Warningf("node %s do not have the address for %s, dropping a message",
			t.sourceAddress, logutil.DescribeNode(groupID, toNodeID))
		return false
	}
	// get the channel, create it in case it is not in the queue map
	t.mu.Lock()
	ch, ok := t.mu.queues[key]
	if !ok {
		ch = make(chan paxospb.PaxosMsg, rpcSendBufSize)
		t.mu.queues[key] = ch
	}
	t.mu.Unlock()
	if !ok {
		shutdownQueue := func() {
			t.mu.Lock()
			delete(t.mu.queues, key)
			t.mu.Unlock()
		}
		t.stopper.RunWorker(func() {
			t.connectAndProcess(groupID, toNodeID, addr, ch, from)
			shutdownQueue()
		})
	}
	select {
	case ch <- req:
		return true
	default:
		return false
	}
}

func (t *Transport) connectAndProcess(groupID uint64, toNodeID uint64,
	remoteHost string, ch <-chan paxospb.PaxosMsg, from uint64) {
	if err := func() error {
		plog.Infof("Nodehost %s is trying to established a connection to %s",
			t.sourceAddress, remoteHost)
		conn, err := t.paxosRPC.GetConnection(t.ctx, remoteHost)
		if err != nil {
			plog.Errorf("Nodehost %s failed to get a connection to %s, %v",
				t.sourceAddress, remoteHost, err)
			return err
		}
		defer conn.Close()
		return t.processQueue(groupID, toNodeID, ch, conn)
	}(); err != nil {
		plog.Warningf("breaker %s to %s failed, connect and process failed: %s",
			t.sourceAddress, remoteHost, err.Error())
	}
}

func (t *Transport) processQueue(groupID uint64, toNodeID uint64,
	ch <-chan paxospb.PaxosMsg, conn paxosio.IConnection) error {
	idleTimer := time.NewTimer(idleTimeout)
	defer idleTimer.Stop()
	// sz := uint64(0)
	batch := paxospb.MessageBatch{
		SourceAddress: t.sourceAddress,
		BinVer:        paxosio.RPCBinVersion,
	}
	requests := make([]paxospb.PaxosMsg, 0)
	for {
		idleTimer.Reset(idleTimeout)
		select {
		case <-t.stopper.ShouldStop():
			plog.Debugf("stopper stopped, %s",
				logutil.DescribeNode(groupID, toNodeID))
			return nil
		case <-idleTimer.C:
			return nil
		case req := <-ch:
			requests = append(requests, req)
			for done := false; !done; {
				select {
				case req = <-ch:
					requests = append(requests, req)
				default:
					done = true
				}
			}
			twoBatch := false
			if len(requests) == 1 {
				batch.Requests = requests
			} else {
				twoBatch = true
				batch.Requests = requests[:len(requests)-1]
			}

			if err := t.sendMessageBatch(conn, batch); err != nil {
				plog.Warningf("Send batch failed, target node %s (%v), %d",
					logutil.DescribeNode(groupID, toNodeID), err, len(batch.Requests))
				return err
			}
			if twoBatch {
				batch.Requests = []paxospb.PaxosMsg{requests[len(requests)-1]}
				if err := t.sendMessageBatch(conn, batch); err != nil {
					plog.Warningf("Send batch failed, taret node %s (%v), %d",
						logutil.DescribeNode(groupID, toNodeID), err, len(batch.Requests))
					return err
				}
			}
			requests, batch = lazyFree(requests, batch)
			requests = requests[:0]
		}
	}
}

func lazyFree(reqs []paxospb.PaxosMsg,
	mb paxospb.MessageBatch) ([]paxospb.PaxosMsg, paxospb.MessageBatch) {
	if lazyFreeCycle > 0 {
		for i := 0; i < len(reqs); i++ {
			reqs[i].Value = nil
		}
		mb.Requests = []paxospb.PaxosMsg{}
	}
	return reqs, mb
}

func (t *Transport) sendMessageBatch(conn paxosio.IConnection,
	batch paxospb.MessageBatch) error {
	return conn.SendMessageBatch(batch)
}

func createTransportRPC(nhConfig config.NodeHostConfig,
	requestHandler paxosio.RequestHandler) paxosio.IPaxosRPC {
	var factory config.PaxosRPCFactoryFunc
	if nhConfig.PaxosRPCFactory != nil {
		factory = nhConfig.PaxosRPCFactory
	} else {
		factory = NewTCPTransport
	}
	return factory(nhConfig, requestHandler)
}

func getDialTimeoutSecond() uint64 {
	return atomic.LoadUint64(&dialTimeoutSecond)
}
