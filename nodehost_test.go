package paxos

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/tests"
	"github.com/LiuzhouChan/go-paxos/internal/transport"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	"github.com/LiuzhouChan/go-paxos/statemachine"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var (
	singleNodeHostTestAddr = "localhost:26000"
	singleNodeHostTestDir  = "single_nodehost_test_dir_safe_to_delete"
)

func ExampleNewNodeHost() {
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "go-paxos",
		RTTMillisecond: 200,
		PaxosAddress:   "myhostname:5012",
	}
	nh := NewNodeHost(nhc)
	log.Printf("nodehost created, running on %s", nh.PaxosAddress())
}

func ExampleNodeHost_StartGroup() {
	nhc := config.NodeHostConfig{
		WALDir:         "wal",
		NodeHostDir:    "go-paxos",
		RTTMillisecond: 200,
		PaxosAddress:   "myhostname:5012",
	}
	// Creates a nodehost instance using the above NodeHostConfig instnace.
	nh := NewNodeHost(nhc)
	// config for paxos
	rc := config.Config{
		NodeID:         1,
		GroupID:        1,
		AskForLearnRTT: 10,
	}
	peers := make(map[uint64]string)
	peers[100] = "myhostname1:5012"
	peers[200] = "myhostname2:5012"
	peers[300] = "myhostname3:5012"
	// Use this NO-OP data store in this example
	NewStateMachine := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
		return &tests.NoOP{}
	}
	if err := nh.StartGroup(peers, false, NewStateMachine, rc); err != nil {
		log.Fatalf("failed to add group, %v\n", err)
	}
}

func ExampleNewNodeHost_Propose(nh *NodeHost) {
	rs, err := nh.Propose(100, []byte("test-data"), 2000*time.Millisecond)
	if err != nil {
		return
	}
	defer rs.Release()
	s := <-rs.CompletedC
	if s.Timeout() {

	} else if s.Completed() {

	} else if s.Terminated() {

	}
}

func getTestNodeHostConfig() *config.NodeHostConfig {
	return &config.NodeHostConfig{
		WALDir:         singleNodeHostTestDir,
		NodeHostDir:    singleNodeHostTestDir,
		RTTMillisecond: 50,
		PaxosAddress:   "localhost:1111",
	}
}

type noopLogDB struct {
}

func (n *noopLogDB) Name() string                                            { return "noopLogDB" }
func (n *noopLogDB) Close()                                                  {}
func (n *noopLogDB) GetLogDBThreadContext() paxosio.IContext                 { return nil }
func (n *noopLogDB) HasNodeInfo(groupID uint64, nodeID uint64) (bool, error) { return true, nil }
func (n *noopLogDB) CreateNodeInfo(groupID uint64, nodeID uint64) error      { return nil }
func (n *noopLogDB) ListNodeInfo() ([]paxosio.NodeInfo, error)               { return nil, nil }
func (n *noopLogDB) SaveBootstrapInfo(groupID uint64, nodeID uint64, bs paxospb.Bootstrap) error {
	return nil
}
func (n *noopLogDB) GetBootstrapInfo(groupID uint64, nodeID uint64) (*paxospb.Bootstrap, error) {
	return nil, nil
}
func (n *noopLogDB) SavePaxosState(updates []paxospb.Update, ctx paxosio.IContext) error { return nil }
func (n *noopLogDB) IterateEntries(groupID uint64, nodeID uint64, low uint64,
	high uint64) ([]paxospb.Entry, error) {
	return nil, nil
}
func (n *noopLogDB) ReadPaxosState(groupID uint64, nodeID uint64,
	lastInstance uint64) (*paxosio.PaxosState, error) {
	return nil, nil
}
func (n *noopLogDB) RemoveEntriesTo(groupID uint64, nodeID uint64, index uint64) error { return nil }

func TestLogDBCanBeExtended(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	ldb := &noopLogDB{}
	c.LogDBFactory = func([]string, []string) (paxosio.ILogDB, error) {
		return ldb, nil
	}
	nh := NewNodeHost(*c)
	defer nh.Stop()
	if nh.logdb.Name() != ldb.Name() {
		t.Errorf("logdb type name %s, expect %s", nh.logdb.Name(), ldb.Name())
	}
}

func TestTCPTransportIsUsedByDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	nh := NewNodeHost(*c)
	defer nh.Stop()
	tt := nh.transport.(*transport.Transport)
	if tt.GetPaxosRPC().Name() != transport.TCPPaxosRPCName {
		t.Errorf("paxos rpc type name %s, expect %s",
			tt.GetPaxosRPC().Name(), transport.TCPPaxosRPCName)
	}
}

func TestPaxosRPCCanBeExtended(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer os.RemoveAll(singleNodeHostTestDir)
	os.RemoveAll(singleNodeHostTestDir)
	c := getTestNodeHostConfig()
	c.PaxosRPCFactory = transport.NewNOOPTransport
	nh := NewNodeHost(*c)
	defer nh.Stop()
	tt := nh.transport.(*transport.Transport)
	if tt.GetPaxosRPC().Name() != transport.NOOPPaxosName {
		t.Errorf("paxos rpc type name %s, expect %s",
			tt.GetPaxosRPC().Name(), transport.NOOPPaxosName)
	}
}

type PST struct {
	mu       sync.Mutex
	stopped  bool
	saved    bool
	restored bool
	slowSave bool
}

func (n *PST) setRestored(v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.restored = v
}

func (n *PST) getRestored() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.restored
}

func (n *PST) Close() {}

// Lookup locally looks up the data.
func (n *PST) Lookup(key []byte) []byte {
	return make([]byte, 1)
}

// Update updates the object.
func (n *PST) Update(data []byte) uint64 {
	return uint64(len(data))
}

// GetHash returns a uint64 value representing the current state of the object.
func (n *PST) GetHash() uint64 {
	// the hash value is always 0, so it is of course always consistent
	return 0
}

func createSingleNodeTestNodeHost(addr string,
	datadir string, slowSave bool) (*NodeHost, *PST, error) {
	// config for raft
	rc := config.Config{
		NodeID:         uint64(1),
		GroupID:        2,
		AskForLearnRTT: 10,
	}
	peers := make(map[uint64]string)
	peers[1] = addr
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 50,
		PaxosAddress:   peers[1],
	}
	nh := NewNodeHost(nhc)
	var pst *PST
	newPST := func(groupID uint64, nodeID uint64) statemachine.IStateMachine {
		pst = &PST{slowSave: slowSave}
		return pst
	}
	if err := nh.StartGroup(peers, false, newPST, rc); err != nil {
		return nil, nil, err
	}
	return nh, pst, nil
}

func TestJoinedGroupCanBeRestartedOrJoinedAgain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	datadir := singleNodeHostTestDir
	rc := config.Config{
		NodeID:         uint64(1),
		GroupID:        2,
		AskForLearnRTT: 10,
	}
	peers := make(map[uint64]string)
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 50,
		PaxosAddress:   singleNodeHostTestAddr,
	}
	nh := NewNodeHost(nhc)
	defer nh.Stop()
	newPST := func(groupID uint64, nodeID uint64) statemachine.IStateMachine {
		return &PST{}
	}
	if err := nh.StartGroup(peers, true, newPST, rc); err != nil {
		t.Fatalf("failed to join the group")
	}
	if err := nh.StopGroup(2); err != nil {
		t.Fatalf("failed to stop the group")
	}
	if err := nh.StartGroup(peers, true, newPST, rc); err != nil {
		t.Fatalf("failed to join the group again")
	}
	if err := nh.StopGroup(2); err != nil {
		t.Fatalf("failed to stop the cluster")
	}
	if err := nh.StartGroup(peers, false, newPST, rc); err != nil {
		t.Fatalf("failed to restartthe cluster again")
	}
}

func singleNodeHostTest(t *testing.T, tf func(t *testing.T, nh *NodeHost)) {
	defer leaktest.AfterTest(t)()
	os.RemoveAll(singleNodeHostTestDir)
	nh, _, err := createSingleNodeTestNodeHost(singleNodeHostTestAddr,
		singleNodeHostTestDir, false)
	if err != nil {
		t.Fatalf("failed to create nodehost %v", err)
	}
	time.Sleep(time.Millisecond * 300)
	defer os.RemoveAll(singleNodeHostTestDir)
	defer nh.Stop()
	tf(t, nh)
}

func TestNodeHostSyncIOAPIs(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		v, err := nh.SyncPropose(ctx, 2, make([]byte, 128))
		if err != nil {
			t.Errorf("make proposal failed %v", err)
		}
		if v != 128 {
			t.Errorf("unexpected result")
		}
		data, err := nh.ReadLocalNode(2, make([]byte, 128))
		if err != nil {
			t.Errorf("make linearizable read failed %v", err)
		}
		if len(data) == 0 {
			t.Errorf("failed to get result")
		}
		if err := nh.StopGroup(2); err != nil {
			t.Errorf("failed to stop cluster 2 %v", err)
		}
	}
	singleNodeHostTest(t, tf)
}
