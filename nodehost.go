package paxos

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/logdb"
	"github.com/LiuzhouChan/go-paxos/internal/rsm"
	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/internal/transport"
	"github.com/LiuzhouChan/go-paxos/internal/utils/lang"
	"github.com/LiuzhouChan/go-paxos/internal/utils/logutil"
	"github.com/LiuzhouChan/go-paxos/internal/utils/stringutil"
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
	monitorInterval          = 100 * time.Millisecond
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

func (nh *NodeHost) getGroupAndQueueNotLocked(groupID uint64) (*node,
	*server.MessageQueue, bool) {
	nh.groupMu.RLock()
	defer nh.groupMu.RUnlock()
	v, ok := nh.getGroupNotLocked(groupID)
	if !ok {
		return nil, nil, false
	}
	q, ok := nh.groupMu.requests[groupID]
	if !ok {
		return nil, nil, false
	}
	return v, q, true
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

func (nh *NodeHost) stopNode(groupID uint64, nodeID uint64, nodeCheck bool) error {
	nh.groupMu.Lock()
	defer nh.groupMu.Unlock()
	v, ok := nh.groupMu.groups.Load(groupID)
	if !ok {
		return ErrGroupNotFound
	}
	group := v.(*node)
	if nodeCheck && group.nodeID != nodeID {
		return ErrGroupNotFound
	}
	nh.groupMu.groups.Delete(groupID)
	delete(nh.groupMu.requests, groupID)
	nh.groupMu.gsi++
	group.notifyOffloaded(rsm.FromNodeHost)
	return nil
}

func (nh *NodeHost) initialize(ctx context.Context,
	nhConfig config.NodeHostConfig) error {
	nh.execEngine = newExecEngine(nh, nh.serverCtx, nh.logdb, nh.sendNoOPMessage)
	nh.setInitialized()
	return nil
}

func (nh *NodeHost) tickWorkerMain() {
	count := uint64(0)
	idx := uint64(0)
	nodes := make([]*node, 0)
	qs := make(map[uint64]*server.MessageQueue)
	tf := func() bool {
		count++
		nh.increateTick()
		if count%nh.nhConfig.RTTMillisecond == 0 {
			// one RTT
			idx, nodes, qs = nh.getCurrentGroups(idx, nodes, qs)

		}
		return false
	}
	lang.RunTicker(time.Millisecond, tf, nh.stopper.ShouldStop(), nil)
}

func (nh *NodeHost) getCurrentGroups(index uint64,
	groups []*node, queues map[uint64]*server.MessageQueue) (uint64,
	[]*node, map[uint64]*server.MessageQueue) {
	newIndex := nh.getGroupSetIndex()
	if newIndex == index {
		return index, groups, queues
	}
	newGroups := groups[:0]
	newQueues := make(map[uint64]*server.MessageQueue)
	nh.forEachGroup(func(gid uint64, node *node) bool {
		newGroups = append(newGroups, node)
		v, ok := nh.groupMu.requests[gid]
		if !ok {
			panic("inconsistent received messageC map")
		}
		newQueues[gid] = v
		return true
	})
	return newIndex, newGroups, newQueues
}

func (nh *NodeHost) asyncSendPaxosRequest(msg paxospb.PaxosMsg) {
	nh.transport.ASyncSend(msg)
}

func (nh *NodeHost) sendTickMessage(groups []*node,
	queues map[uint64]*server.MessageQueue) {
	m := paxospb.PaxosMsg{MsgType: paxospb.LocalTick}
	for _, n := range groups {
		q, ok := queues[n.groupID]
		if !ok || nh.initialized() {
			continue
		}
		q.Add(m)
		nh.execEngine.setNodeReady(n.groupID)
	}
}

func (nh *NodeHost) sendNoOPMessage(groupID, NodeID uint64) {
	batch := paxospb.MessageBatch{
		Requests: make([]paxospb.PaxosMsg, 0),
	}
	msg := paxospb.PaxosMsg{
		MsgType: paxospb.NoOP,
		To:      NodeID,
		From:    NodeID,
		GroupId: groupID,
	}
	batch.Requests = append(batch.Requests, msg)
	nh.msgHandler.HandleMessageBatch(batch)
}

func (nh *NodeHost) closeStoppedGroups() {
	chans := make([]<-chan struct{}, 0)
	keys := make([]uint64, 0)
	nodeIDs := make([]uint64, 0)
	nh.forEachGroup(func(gid uint64, node *node) bool {
		chans = append(chans, node.shouldStop())
		keys = append(keys, gid)
		nodeIDs = append(nodeIDs, node.nodeID)
		return true
	})
	if len(chans) == 0 {
		return
	}

	cases := make([]reflect.SelectCase, len(chans)+1)
	for i, ch := range chans {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}
	cases[len(chans)] = reflect.SelectCase{Dir: reflect.SelectDefault}
	chosen, _, ok := reflect.Select(cases)
	for !ok && chosen < len(keys) {
		groupID := keys[chosen]
		nodeID := nodeIDs[chosen]
		if err := nh.StopNode(groupID, nodeID); err != nil {
			plog.Errorf("failed to remove group %d", groupID)
		}
	}
}

func (nh *NodeHost) nodeMonitorMain(ctx context.Context,
	nhConfig config.NodeHostConfig) {
	count := uint64(0)
	tf := func() bool {
		count++
		nh.closeStoppedGroups()
		return false
	}
	lang.RunTicker(monitorInterval, tf, nh.stopper.ShouldStop(), nil)
}

//StopGroup ...
func (nh *NodeHost) StopGroup(groupID uint64) error {
	return nh.stopNode(groupID, 0, false)
}

//StopNode ...
func (nh *NodeHost) StopNode(groupID uint64, nodeID uint64) error {
	return nh.stopNode(groupID, nodeID, true)
}

func (nh *NodeHost) initialized() bool {
	select {
	case <-nh.initializedC:
		return true
	default:
		return false
	}
}

func (nh *NodeHost) waitUntilInitialized() {
	<-nh.initializedC
}

func (nh *NodeHost) setInitialized() {
	close(nh.initializedC)
}

func (nh *NodeHost) increateTick() {
	atomic.AddUint64(&nh.tick, 1)
}

func (nh *NodeHost) getTick() uint64 {
	return atomic.LoadUint64(&nh.tick)
}

func (nh *NodeHost) getGroupSetIndex() uint64 {
	nh.groupMu.RLock()
	v := nh.groupMu.gsi
	nh.groupMu.RUnlock()
	return v
}

func (nh *NodeHost) describe() string {
	return nh.PaxosAddress()
}

func (nh *NodeHost) bootstrapGroup(nodes map[uint64]string,
	join bool, config config.Config) (map[uint64]string, bool, error) {
	binfo, err := nh.logdb.GetBootstrapInfo(config.GroupID, config.NodeID)
	if err == paxosio.ErrNoBootstrapInfo {
		var members map[uint64]string
		if !join {
			members = nodes
		}
		bootstrap := paxospb.Bootstrap{
			Join:      join,
			Addresses: make(map[uint64]string),
		}
		for nid, addr := range nodes {
			bootstrap.Addresses[nid] = stringutil.CleanAddress(addr)
		}
		err = nh.logdb.SaveBootstrapInfo(config.GroupID, config.NodeID, bootstrap)
		plog.Infof("bootstrap for %s found node not bootstrapped, %v",
			logutil.DescribeNode(config.GroupID, config.NodeID), members)
		return members, !join, err
	}
	plog.Infof("bootstrap for %s returns %v", logutil.DescribeNode(config.GroupID, config.NodeID),
		binfo.Addresses)
	return binfo.Addresses, !join, nil
}

func (nh *NodeHost) startGroup(nodes map[uint64]string,
	join bool,
	createStateMachine rsm.ManagedStateMachineFactory,
	stopc chan struct{},
	config config.Config) error {
	plog.Infof("start group called for %s, join %t, nodes %v",
		logutil.DescribeNode(config.GroupID, config.NodeID), join, nodes)
	nh.groupMu.Lock()
	defer nh.groupMu.Unlock()
	if nh.groupMu.stopped {
		return ErrSystemStopped
	}
	if _, ok := nh.groupMu.groups.Load(config.GroupID); ok {
		return ErrGroupAlreadyExist
	}
	if join && len(nodes) > 0 {
		plog.Errorf("trying to join %s with initial member list %v",
			logutil.DescribeNode(config.GroupID, config.NodeID), nodes)
		return ErrInvalidGroupSettings
	}
	address, _, err := nh.bootstrapGroup(nodes, join, config)
	if err == ErrInvalidGroupSettings {
		return err
	}
	if err != nil {
		panic(err)
	}
	plog.Infof("bootstrap for %s returned address list %v",
		logutil.DescribeNode(config.GroupID, config.NodeID), address)
	for k, v := range address {
		if k != config.NodeID {
			plog.Infof("AddNode called with node %s, addr %s",
				logutil.DescribeNode(config.GroupID, config.NodeID), address)
			nh.nodes.AddNode(config.GroupID, k, v)
		}
	}
	return nil
}

type nodeUser struct {
	nh           *NodeHost
	node         *node
	setNodeReady func(groupID uint64)
}

func (nu *nodeUser) Propose(groupID uint64, cmd []byte,
	timeout time.Duration) (*RequestState, error) {
	req, err := nu.node.propose(cmd, nil, timeout)
	nu.setNodeReady(groupID)
	return req, err
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
	nh := h.nh
	for _, req := range msg.Requests {
		_, q, ok := nh.getGroupAndQueueNotLocked(req.GroupId)
		if ok {
			// here we only deal with the regular message
			if added, stopped := q.Add(req); !added || stopped {
				plog.Warningf("dropped an incomming message")
			}
		}
		nh.execEngine.setNodeReady(req.GroupId)
	}
}
