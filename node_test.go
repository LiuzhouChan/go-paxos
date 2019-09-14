package paxos

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/logdb"
	"github.com/LiuzhouChan/go-paxos/internal/rsm"
	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/internal/tests"
	"github.com/LiuzhouChan/go-paxos/internal/transport"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

const (
	paxosTestTopDir           = "raft_node_test_safe_to_delete"
	logdbDir                  = "logdb_test_dir_safe_to_delete"
	lowLatencyLogDBDir        = "logdb_ll_test_dir_safe_to_delete"
	testGroupID        uint64 = 1100
	tickMillisecond    uint64 = 50
)

func getMemberNodes(r *rsm.StateMachine) []uint64 {
	m, _, _ := r.GetMembership()
	n := make([]uint64, 0)
	for nid := range m {
		n = append(n, nid)
	}
	return n
}

type testMessageRouter struct {
	groupID      uint64
	msgReceiveCh map[uint64]*server.MessageQueue
	dropRate     uint8
}

func mustComplete(rs *RequestState, t *testing.T) {
	select {
	case v := <-rs.CompletedC:
		if !v.Completed() {
			t.Fatalf("got %d, want %d", v, requestCompleted)
		}
	default:
		t.Fatalf("failed to complete the proposal")
	}
}

func mustReject(rs *RequestState, t *testing.T) {
	select {
	case v := <-rs.CompletedC:
		if !v.Rejected() {
			t.Errorf("got %d, want %d", v, requestRejected)
		}
	default:
		t.Errorf("failed to complete the add node request")
	}
}

func newTestMessageRouter(groupID uint64,
	nodeIDList []uint64) *testMessageRouter {
	chMap := make(map[uint64]*server.MessageQueue)
	for _, nodeID := range nodeIDList {
		ch := server.NewMessageQueue(1000, false, 0)
		chMap[nodeID] = ch
	}
	rand.Seed(time.Now().UnixNano())
	return &testMessageRouter{msgReceiveCh: chMap, groupID: groupID}
}

func (r *testMessageRouter) shouldDrop(msg paxospb.PaxosMsg) bool {

	return false
}

func (r *testMessageRouter) sendMessage(msg paxospb.PaxosMsg) {
	if msg.GroupID != r.groupID {
		panic("group id does not match")
	}
	if r.shouldDrop(msg) {
		return
	}
	if q, ok := r.msgReceiveCh[msg.To]; ok {
		q.Add(msg)
	}
}

func (r *testMessageRouter) getMessageReceiveChannel(groupID uint64,
	nodeID uint64) *server.MessageQueue {
	if groupID != r.groupID {
		panic("cluster id does not match")
	}
	ch, ok := r.msgReceiveCh[nodeID]
	if !ok {
		panic("node id not found in the test msg router")
	}
	return ch
}

func (r *testMessageRouter) addChannel(nodeID uint64, q *server.MessageQueue) {
	r.msgReceiveCh[nodeID] = q
}

func cleanupTestDir() {
	os.RemoveAll(paxosTestTopDir)
}

func doGetTestPaxosNodes(startID uint64, count int,
	ldb paxosio.ILogDB) ([]*node, []*rsm.StateMachine, *testMessageRouter, paxosio.ILogDB) {
	nodes := make([]*node, 0)
	smList := make([]*rsm.StateMachine, 0)
	nodeIDList := make([]uint64, 0)

	// peers map
	peers := make(map[uint64]string)
	endID := startID + uint64(count-1)
	for i := startID; i <= endID; i++ {
		nodeIDList = append(nodeIDList, i)
		peers[i] = fmt.Sprintf("peer:%d", 12345+i)
	}
	// pools
	requestStatePool := &sync.Pool{}
	requestStatePool.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = requestStatePool
		return obj
	}
	var err error
	if ldb == nil {
		nodeLoadDir := filepath.Join(paxosTestTopDir, logdbDir)
		nodeLowLatencyLogDir := filepath.Join(paxosTestTopDir, lowLatencyLogDBDir)
		os.MkdirAll(nodeLoadDir, 0755)
		os.MkdirAll(nodeLowLatencyLogDir, 0755)
		ldb, err = logdb.OpenLogDB([]string{nodeLoadDir}, []string{nodeLowLatencyLogDir})
		if err != nil {
			plog.Panicf("failed to open logdb, %v", err)
		}
	}
	//message router
	router := newTestMessageRouter(testGroupID, nodeIDList)
	for i := startID; i <= endID; i++ {
		// create the sm
		sm := &tests.NoOP{}
		ds := rsm.NewNativeStateMachine(sm, make(chan struct{}))
		// node registry
		nr := transport.NewNodes(settings.Soft.StreamConnections)
		config := config.Config{
			NodeID:  uint64(i),
			GroupID: testGroupID,
		}
		addr := fmt.Sprintf("a%d", i)
		ch := router.getMessageReceiveChannel(testGroupID, uint64(i))
		node := newNode(
			addr,
			peers,
			true,
			ds,
			func(uint64) {},
			router.sendMessage,
			ch,
			make(chan struct{}),
			nr,
			requestStatePool,
			config,
			tickMillisecond,
			ldb)
		nodes = append(nodes, node)
		smList = append(smList, node.sm)
	}
	return nodes, smList, router, ldb
}

var ptc paxosio.IContext

func step(nodes []*node) bool {
	hasEvent := false
	nodeUpdates := make([]paxospb.Update, 0)
	activeNodes := make([]*node, 0)
	for _, node := range nodes {
		if !node.initialized() {
			node.setInitialStatus(0)
		}
		if node.initialized() {
			if node.handleEvents() {
				hasEvent = true
				ud, ok := node.getUpdate()
				if ok {
					nodeUpdates = append(nodeUpdates, ud)
					activeNodes = append(activeNodes, node)
				}
			}
		}
	}
	for idx, ud := range nodeUpdates {
		node := activeNodes[idx]
		node.applyPaxosUpdates(ud)
		node.sendMessages(ud.Messages)
	}
	if ptc == nil {
		ptc = nodes[0].logdb.GetLogDBThreadContext()
	} else {
		ptc.Reset()
	}
	// persistent state and entries
	if err := nodes[0].logdb.SavePaxosState(nodeUpdates, ptc); err != nil {
		panic(err)
	}
	for idx, ud := range nodeUpdates {
		node := activeNodes[idx]
		running := node.processPaxosUpdate(ud)
		node.commitPaxosUpdate(ud)
		if running {
			node.sm.Handle(make([]rsm.Commit, 0))
		}
	}
	return hasEvent
}
