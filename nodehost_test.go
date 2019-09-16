package paxos

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/tests"
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
