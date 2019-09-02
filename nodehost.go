package paxos

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/logdb"
	"github.com/LiuzhouChan/go-paxos/internal/rsm"
	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/internal/transport"
	"github.com/LiuzhouChan/go-paxos/internal/utils/syncutil"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	"github.com/LiuzhouChan/go-paxos/statemachine"
)

const (
	//PaxosMajor ...
	PaxosMajor = 0
	//PaxosMinor ...
	PaxosMinor = 0
	//PaxosPatch ...
	PaxosPatch = 1
	// DEVVersion is a boolean value to indicate whether this is a dev version
	DEVVersion = true
)

var (
	delaySampleRatio  uint64 = settings.Soft.LatencySampleRatio
	streamConnections        = settings.Soft.StreamConnections
	rsPoolSize               = settings.Soft.NodeHostSyncPoolSize
)

var (
	ErrGroupNotFound        = errors.New("group not found")
	ErrGroupAlreadyExist    = errors.New("group already exist")
	ErrInvalidGroupSettings = errors.New("group settings are invalid")
	ErrDeadlineNotSet       = errors.New("deadline not set")
	ErrInvalidDeadline      = errors.New("invalid deadline")
)

//NodeHost ...
type NodeHost struct {
	tick     uint64
	msgCount uint64
	groupMu  struct {
		sync.RWMutex
		stopped  bool
		gsi      uint64
		groups   sync.Map
		requests map[uint64]*server.MessageQueue
	}
	cancel           context.CancelFunc
	serverCtx        *server.Context
	nhConfig         config.NodeHostConfig
	stopper          *syncutil.Stopper
	duStopper        *syncutil.Stopper
	nodes            *transport.Nodes
	rsPool           []*sync.Pool
	execEngine       *execEngine
	logdb            paxosio.ILogDB
	transport        transport.ITransport
	msgHandler       *messageHandler
	initializedC     chan struct{}
	transportLatency *sample
}

//NewNodeHost ...
func NewNodeHost(nhConfig config.NodeHostConfig) *NodeHost {
	nh := &NodeHost{
		serverCtx:        server.NewContext(nhConfig),
		nhConfig:         nhConfig,
		stopper:          syncutil.NewStopper(),
		duStopper:        syncutil.NewStopper(),
		nodes:            transport.NewNodes(streamConnections),
		initializedC:     make(chan struct{}),
		transportLatency: newSample(),
	}
	nh.msgHandler = newNodeHostMessageHandler(nh)
	nh.groupMu.requests = make(map[uint64]*server.MessageQueue)

	return nh
}

//NodeHostConfig ...
func (nh *NodeHost) NodeHostConfig() config.NodeHostConfig {
	return nh.nhConfig
}

//PaxosAddress ...
func (nh *NodeHost) PaxosAddress() string {
	return nh.nhConfig.PaxosAddress
}

//StartGroup ...
func (nh *NodeHost) StartGroup(nodes map[uint64]string, join bool,
	createStateMachine func(uint64, uint64) statemachine.IStateMachine,
	config config.Config) error {
	stopc := make(chan struct{})
	cf := func(groupID uint64, nodeID uint64,
		done <-chan struct{}) rsm.IManagedStateMachine {
		sm := createStateMachine(groupID, nodeID)
		return rsm.NewNativeStateMachine(sm, done)
	}
	return nh.startGroup(nodes, join, cf, stopc, config)
}

//SyncPropose ...
func (nh *NodeHost) SyncPropose(ctx context.Context, groupID uint64,
	cmd []byte) (uint64, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return 0, err
	}
	rs, err := nh.Propose(groupID, cmd, timeout)
	if err != nil {
		return 0, err
	}
	select {
	case s := <-rs.CompletedC:
		if s.Timeout() {
			return 0, ErrTimeout
		} else if s.Completed() {
			rs.Release()
			return s.GetResult(), nil
		} else if s.Terminated() {
			return 0, ErrGroupClosed
		} else if s.Rejected() {
			return 0, ErrInvalidSession
		}
		panic("unknow CompletedC value")
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return 0, ErrCanceled
		} else if ctx.Err() == context.DeadlineExceeded {
			return 0, ErrTimeout
		}
		panic("unknow ctx error")
	}
}

// ReadLocalNode queries the paxos node identified by the input RequestState
// instance.
func (nh *NodeHost) ReadLocalNode(rs *RequestState, query []byte) ([]byte, error) {
	if rs.node == nil {
		panic("invalid rs")
	}
	data, err := rs.node.sm.Lookup(query)
	if err == rsm.ErrGroupClosed {
		return nil, ErrGroupClosed
	}
	return data, err
}

func (nh *NodeHost) getGroupNotLocked(groupID uint64) (*node, bool) {
	v, ok := nh.groupMu.groups.Load(groupID)
	if !ok {
		return nil, false
	}
	return v.(*node), true
}

func (nh *NodeHost) getGroup(groupID uint64) (*node, bool) {
	nh.groupMu.RLock()
	v, ok := nh.groupMu.groups.Load(groupID)
	nh.groupMu.RUnlock()
	if !ok {
		return nil, false
	}
	return v.(*node), true
}

func (nh *NodeHost) forEachGroupRun(bf func() bool, af func() bool,
	f func(uint64, *node) bool) {
	nh.groupMu.RLock()
	defer nh.groupMu.RUnlock()
	if bf != nil {
		if !bf() {
			return
		}
	}
	nh.groupMu.groups.Range(func(k, v interface{}) bool {
		return f(k.(uint64), v.(*node))
	})
	if af != nil {
		if !af() {
			return
		}
	}
}

func (nh *NodeHost) forEachGroup(f func(uint64, *node) bool) {
	nh.forEachGroupRun(nil, nil, f)
}

//Propose ...
func (nh *NodeHost) Propose(grouID uint64, cmd []byte,
	timeout time.Duration) (*RequestState, error) {
	return nh.propose(grouID, cmd, nil, timeout)
}

func (nh *NodeHost) propose(groupID uint64, cmd []byte, handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	c, ok := nh.groupMu.groups.Load(groupID)
	if !ok {
		return nil, ErrGroupNotFound
	}
	v := c.(*node)
	req, err := v.propose(cmd, handler, timeout)
	nh.execEngine.setNodeReady(groupID)
	return req, err
}

func (nh *NodeHost) startGroup(nodes map[uint64]string,
	join bool, createStateMachine rsm.ManagedStateMachineFactory,
	stopc chan struct{}, config config.Config) error {
	return nil
}

func (nh *NodeHost) createPools() {
	nh.rsPool = make([]*sync.Pool, rsPoolSize)
	for i := uint64(0); i < rsPoolSize; i++ {
		p := &sync.Pool{}
		p.New = func() interface{} {
			obj := &RequestState{}
			obj.CompletedC = make(chan RequestResult, 1)
			obj.pool = p
			return obj
		}
		nh.rsPool[i] = p
	}
}

func (nh *NodeHost) createLogDB(nhConfig config.Config) {
	nhDirs, walDisr := nh.serverCtx.CreateNodeHostDir()
	nh.serverCtx.CheckNodeHostDir(nh.nhConfig.PaxosAddress)
	var factory config.LogDBFactoryFunc
	factory = logdb.OpenLogDB
	logdb, err := factory(nhDirs, walDisr)
	if err != nil {
		panic(err)
	}
	plog.Infof("logdb type name: %s", logdb.Name())
	nh.logdb = logdb
}

func (nh *NodeHost) createTransport() {
	nh.transport = transport.NewTransport(nh.nhConfig, nh.serverCtx, nh.nodes)
	nh.transport.SetMessageHandler(nh.msgHandler)
}

func getTimeoutFromContext(ctx context.Context) (time.Duration, error) {
	d, ok := ctx.Deadline()
	if !ok {
		return 0, ErrDeadlineNotSet
	}
	now := time.Now()
	if now.After(d) {
		return 0, ErrInvalidDeadline
	}
	return d.Sub(now), nil
}

type messageHandler struct {
	nh *NodeHost
}

func newNodeHostMessageHandler(nh *NodeHost) *messageHandler {
	return &messageHandler{nh: nh}
}

func (h *messageHandler) HandleMessageBatch(msg paxospb.MessageBatch) {

}
