package paxos

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/logdb"
	"github.com/LiuzhouChan/go-paxos/internal/tests"
	"github.com/LiuzhouChan/go-paxos/internal/transport"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	"github.com/LiuzhouChan/go-paxos/statemachine"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var (
	singleNodeHostTestAddr = "localhost:26000"
	singleNodeHostTestDir  = "single_nodehost_test_dir_safe_to_delete"
	rdbTestDirectory       = "rdb_test_dir_safe_to_delete"
)

func getNewTestDB(dir string, lldir string) paxosio.ILogDB {
	d := filepath.Join(rdbTestDirectory, dir)
	lld := filepath.Join(rdbTestDirectory, lldir)
	os.MkdirAll(d, 0777)
	os.MkdirAll(lld, 0777)
	db, err := logdb.OpenLogDB([]string{d}, []string{lld})
	if err != nil {
		panic(err.Error())
	}
	return db
}

func deleteTestRDB() {
	os.RemoveAll(rdbTestDirectory)
}

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

func TestNodeHostGetNodeUser(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		n, err := nh.GetNodeUser(2)
		if err != nil {
			t.Errorf("failed to get NodeUser")
		}
		if n == nil {
			t.Errorf("got a nil NodeUser")
		}
		n, err = nh.GetNodeUser(123)
		if err != ErrGroupNotFound {
			t.Errorf("didn't return expected err")
		}
		if n != nil {
			t.Errorf("got unexpected node user")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostNodeUserPropose(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		n, err := nh.GetNodeUser(2)
		if err != nil {
			t.Errorf("failed to get NodeUser")
		}
		rs, err := n.Propose(2, make([]byte, 16), time.Second)
		if err != nil {
			t.Errorf("fialed to make propose %v", err)
		}
		v := <-rs.CompletedC
		if !v.Completed() {
			t.Errorf("failed to complete proposal")
		}
	}
	singleNodeHostTest(t, tf)
}

func TestNodeHostHasNodeInfo(t *testing.T) {
	tf := func(t *testing.T, nh *NodeHost) {
		if ok := nh.HasNodeInfo(2, 1); !ok {
			t.Errorf("node info missing")
		}
		if ok := nh.HasNodeInfo(2, 2); ok {
			t.Errorf("unexpected node info")
		}
	}
	singleNodeHostTest(t, tf)
}

/******************************************************************************
* Benchmarks
******************************************************************************/

func benchmarkNoPool128Allocs(b *testing.B, sz uint64) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := make([]byte, sz)
			b.SetBytes(int64(sz))
			if uint64(len(m)) < sz {
				b.Errorf("len(m) < %d", sz)
			}
		}
	})
}

func BenchmarkNoPool128Allocs512Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 512)
}

func BenchmarkNoPool128Allocs15Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 15)
}

func BenchmarkNoPool128Allocs2Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 2)
}

func BenchmarkNoPool128Allocs16Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 16)
}

func BenchmarkNoPool128Allocs17Bytes(b *testing.B) {
	benchmarkNoPool128Allocs(b, 17)
}

// FIXME: opti it, but now it is a list dequeue
func BenchmarkAddToEntryQueue(b *testing.B) {
	b.ReportAllocs()
	q := newEntryQueue(1000000)
	entry := paxospb.Entry{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.add(entry)
			q.get()
		}
	})
}

func benchmarkProposeN(b *testing.B, sz int) {
	b.ReportAllocs()
	data := make([]byte, sz)
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	q := newEntryQueue(2048)
	pp := newPendingProposal(p, q, 1, 1, "localhost:9090", 200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			b.SetBytes(int64(sz))
			rs, err := pp.propose(data, nil, time.Second)
			if err != nil {
				b.Errorf("%v", err)
			}
			q.get()
			pp.applied(rs.key, 1, false)
			rs.Release()
		}
	})
}

// BenchmarkPropose16-8 1000000 2014 ns/op  7.94 MB/s  256 B/op  4 allocs/op
// BenchmarkPropose16-8 2000000  997 ns/op  16.03 MB/s 208 B/op  3 allocs/op
func BenchmarkPropose16(b *testing.B) {
	benchmarkProposeN(b, 16)
}

func BenchmarkPropose128(b *testing.B) {
	benchmarkProposeN(b, 128)
}

func BenchmarkPropose1024(b *testing.B) {
	benchmarkProposeN(b, 1024)
}

func BenchmarkPendingProposalNextKey(b *testing.B) {
	b.ReportAllocs()
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.CompletedC = make(chan RequestResult, 1)
		obj.pool = p
		return obj
	}
	q := newEntryQueue(2048)
	pp := newPendingProposal(p, q, 1, 1, "localhost:9090", 200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pp.nextKey()
		}
	})
}

func benchmarkMarshalEntryN(b *testing.B, sz int) {
	b.ReportAllocs()
	e := paxospb.Entry{
		Type: paxospb.ApplicationEntry,
		Key:  123123123,
		AcceptorState: paxospb.AcceptorState{
			InstanceID: 12,
			PromiseBallot: paxospb.BallotNumber{
				ProposalID: 21,
				NodeID:     1,
			},
			AcceptedBallot: paxospb.BallotNumber{
				ProposalID: 21,
				NodeID:     1,
			},
			AccetpedValue: make([]byte, sz),
		},
	}
	data := make([]byte, e.Size())
	for i := 0; i < b.N; i++ {
		n, err := e.MarshalTo(data)
		if n > len(data) {
			b.Errorf("n > len(data)")
		}
		b.SetBytes(int64(n))
		if err != nil {
			b.Errorf("%v", err)
		}
	}
}

func BenchmarkMarshalEntry16(b *testing.B) {
	benchmarkMarshalEntryN(b, 16)
}

func BenchmarkMarshalEntry128(b *testing.B) {
	benchmarkMarshalEntryN(b, 128)
}

func BenchmarkMarshalEntry1024(b *testing.B) {
	benchmarkMarshalEntryN(b, 1024)
}

func BenchmarkReadyGroup(b *testing.B) {
	b.ReportAllocs()
	rc := newReadyGroup()
	b.RunParallel(func(pbt *testing.PB) {
		for pbt.Next() {
			rc.setGroupReady(1)
		}
	})
}

func BenchmarkFSyncLatency(b *testing.B) {
	b.StopTimer()
	l := logger.GetLogger("logdb")
	l.SetLevel(logger.WARNING)
	db := getNewTestDB("db", "lldb")
	defer os.RemoveAll(rdbTestDirectory)
	defer db.Close()
	e := paxospb.Entry{
		Type: paxospb.ApplicationEntry,
		Key:  123123123,
		AcceptorState: paxospb.AcceptorState{
			InstanceID: 12,
			PromiseBallot: paxospb.BallotNumber{
				ProposalID: 21,
				NodeID:     1,
			},
			AcceptedBallot: paxospb.BallotNumber{
				ProposalID: 21,
				NodeID:     1,
			},
			AccetpedValue: make([]byte, 8*1024),
		},
	}
	u := paxospb.Update{
		GroupID:       1,
		NodeID:        1,
		EntriesToSave: []paxospb.Entry{e},
	}
	rdbctx := db.GetLogDBThreadContext()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		if err := db.SavePaxosState([]paxospb.Update{u}, rdbctx); err != nil {
			b.Fatalf("%v", err)
		}
		rdbctx.Reset()
	}
}

func benchmarkSavePaxosState(b *testing.B, sz int) {
	b.ReportAllocs()
	b.StopTimer()
	l := logger.GetLogger("logdb")
	l.SetLevel(logger.WARNING)
	db := getNewTestDB("db", "lldb")
	defer os.RemoveAll(rdbTestDirectory)
	defer db.Close()
	e := paxospb.Entry{
		Type: paxospb.ApplicationEntry,
		Key:  123123123,
		AcceptorState: paxospb.AcceptorState{
			InstanceID: 12,
			PromiseBallot: paxospb.BallotNumber{
				ProposalID: 21,
				NodeID:     1,
			},
			AcceptedBallot: paxospb.BallotNumber{
				ProposalID: 21,
				NodeID:     1,
			},
			AccetpedValue: make([]byte, 8*1024),
		},
	}
	bytes := e.Size() * 128
	u := paxospb.Update{
		GroupID: 1,
		NodeID:  1,
	}
	iidx := e.AcceptorState.InstanceID
	for i := uint64(0); i < 128; i++ {
		e.AcceptorState.InstanceID = iidx + i
		u.EntriesToSave = append(u.EntriesToSave, e)
	}
	b.StartTimer()
	b.RunParallel(func(pbt *testing.PB) {
		rdbctx := db.GetLogDBThreadContext()
		for pbt.Next() {
			rdbctx.Reset()
			if err := db.SavePaxosState([]paxospb.Update{u}, rdbctx); err != nil {
				b.Errorf("%v", err)
			}
			b.SetBytes(int64(bytes))
		}
	})
}

func BenchmarkSaveRaftState16(b *testing.B) {
	benchmarkSavePaxosState(b, 16)
}

func BenchmarkSaveRaftState128(b *testing.B) {
	benchmarkSavePaxosState(b, 128)
}

func BenchmarkSaveRaftState1024(b *testing.B) {
	benchmarkSavePaxosState(b, 1024)
}
