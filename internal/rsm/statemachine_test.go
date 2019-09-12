package rsm

import (
	"os"
	"testing"

	"github.com/LiuzhouChan/go-paxos/internal/tests"
	"github.com/LiuzhouChan/go-paxos/internal/tests/kvpb"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	"github.com/LiuzhouChan/go-paxos/statemachine"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

const (
	testSnapshotterDir = "rsm_test_data_safe_to_delete"
	snapshotFileSuffix = "gbsnap"
)

func removeTestDir() {
	os.RemoveAll(testSnapshotterDir)
}

func createTestDir() {
	removeTestDir()
	os.MkdirAll(testSnapshotterDir, 0755)
}

type testNodeProxy struct {
	addPeer            bool
	removePeer         bool
	reject             bool
	accept             bool
	smResult           uint64
	lastApplied        uint64
	rejected           bool
	ignored            bool
	applyUpdateInvoked bool
	addPeerCount       uint64
	addObserver        bool
	addObserverCount   uint64
}

func newTestNodeProxy() *testNodeProxy {
	return &testNodeProxy{}
}

func (p *testNodeProxy) ApplyUpdate(entry paxospb.Entry,
	result uint64, rejected bool, ignored bool) {
	p.smResult = result
	p.lastApplied = entry.AcceptorState.InstanceID
	p.rejected = rejected
	p.ignored = ignored
	p.applyUpdateInvoked = true
}

func (p *testNodeProxy) SetLastApplied(v uint64) {}

func (p *testNodeProxy) NodeID() uint64  { return 1 }
func (p *testNodeProxy) GroupID() uint64 { return 1 }

func runSMTest(t *testing.T, tf func(t *testing.T, sm *StateMachine)) {
	defer leaktest.AfterTest(t)()
	store := tests.NewKVTest(1, 1)
	ds := NewNativeStateMachine(store, make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	sm := NewStateMachine(ds, nodeProxy)
	tf(t, sm)
}

func runSMTest2(t *testing.T,
	tf func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, store statemachine.IStateMachine)) {
	defer leaktest.AfterTest(t)()
	createTestDir()
	defer removeTestDir()
	store := tests.NewKVTest(1, 1)
	ds := NewNativeStateMachine(store, make(chan struct{}))
	nodeProxy := newTestNodeProxy()
	sm := NewStateMachine(ds, nodeProxy)
	tf(t, sm, ds, nodeProxy, store)
}

func TestStateMachineCanBeCreated(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		if sm.CommitChanBusy() {
			t.Errorf("commitChan busy")
		}
	}
	runSMTest(t, tf)
}

func TestLookupNotAllowedOnClosedGroup(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.aborted = true
		v, err := sm.Lookup(make([]byte, 10))
		if err != ErrGroupClosed {
			t.Errorf("unexpected err value %v", err)
		}
		if v != nil {
			t.Errorf("unexpected result")
		}
	}
	runSMTest(t, tf)
}

func TestGetMembership(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members = &paxospb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			Removed: map[uint64]bool{
				400: true,
				500: true,
				600: true,
			},
			ConfigChangeId: 12345,
		}
		m, r, cid := sm.GetMembership()
		if cid != 12345 {
			t.Errorf("unexpected cid value")
		}
		if len(m) != 2 || len(r) != 3 {
			t.Errorf("len changed")
		}
	}
	runSMTest(t, tf)
}

func TestGetMembershipNodes(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine) {
		sm.members = &paxospb.Membership{
			Addresses: map[uint64]string{
				100: "a100",
				234: "a234",
			},
			Removed: map[uint64]bool{
				400: true,
				500: true,
				600: true,
			},
			ConfigChangeId: 12345,
		}
		n, _, _ := sm.GetMembership()
		if len(n) != 2 {
			t.Errorf("unexpected len")
		}
		_, ok1 := n[100]
		_, ok2 := n[234]
		if !ok1 || !ok2 {
			t.Errorf("unexpected node id")
		}
	}
	runSMTest(t, tf)
}

func TestHandleEmptyEvent(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, store statemachine.IStateMachine) {
		e := paxospb.Entry{
			Type:          paxospb.ApplicationEntry,
			AcceptorState: paxospb.AcceptorState{InstanceID: 234},
		}
		commit := Commit{
			Entries: []paxospb.Entry{e},
		}
		sm.lastApplied = 233
		sm.CommitC() <- commit
		batch := make([]Commit, 0, 8)
		sm.Handle(batch)
		if sm.GetLastApplied() != 234 {
			t.Errorf("last applied %d, want 234", sm.GetLastApplied())
		}
	}
	runSMTest2(t, tf)
}

func getTestKVData() []byte {
	k := "test-key"
	d := "test-value"
	u := kvpb.PBKV{
		Key: &k,
		Val: &d,
	}
	data, err := proto.Marshal(&u)
	if err != nil {
		panic(err)
	}
	return data
}

func TestHandleUpate(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, store statemachine.IStateMachine) {
		data := getTestKVData()
		e1 := paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			AcceptorState: paxospb.AcceptorState{
				InstanceID: 235,
			},
		}
		e2 := paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			AcceptorState: paxospb.AcceptorState{
				InstanceID:    236,
				AccetpedValue: data,
			},
		}
		commit := Commit{
			Entries: []paxospb.Entry{e1, e2},
		}
		sm.lastApplied = 234
		sm.CommitC() <- commit
		// two commits to handle
		batch := make([]Commit, 0, 8)
		sm.Handle(batch)
		if sm.GetLastApplied() != 236 {
			t.Errorf("last applied %d, want 236", sm.GetLastApplied())
		}
		v, ok := store.(*tests.KVTest).KVStore["test-key"]
		if !ok {
			t.Errorf("value not set")
		}
		if v != "test-value" {
			t.Errorf("v: %s, want test-value", v)
		}
		result, err := sm.Lookup([]byte("test-key"))
		if err != nil {
			t.Errorf("lookup failed")
		}
		if string(result) != "test-value" {
			t.Errorf("result %s, want test-value", string(result))
		}
	}
	runSMTest2(t, tf)
}

func applyTestEntry(sm *StateMachine, instanceID uint64, data []byte) paxospb.Entry {
	e := paxospb.Entry{
		AcceptorState: paxospb.AcceptorState{
			InstanceID:    instanceID,
			AccetpedValue: data,
		},
	}
	commit := Commit{
		Entries: []paxospb.Entry{e},
	}
	sm.CommitC() <- commit
	return e
}

func TestNoOPSessionAllowEntryToBeAppliedTwice(t *testing.T) {
	tf := func(t *testing.T, sm *StateMachine, ds IManagedStateMachine,
		nodeProxy *testNodeProxy, store statemachine.IStateMachine) {
		batch := make([]Commit, 0, 8)
		sm.Handle(batch)
		sm.lastApplied = 789
		data := getTestKVData()
		e := applyTestEntry(sm, 790, data)
		// check normal update is accepted and handleped
		sm.Handle(batch)
		if sm.GetLastApplied() != e.AcceptorState.InstanceID {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.AcceptorState.InstanceID)
		}
		storeCount := store.(*tests.KVTest).Count
		// different index as the same entry is proposed again
		e = applyTestEntry(sm, 791, data)
		// check normal update is accepted and handleped
		sm.Handle(batch)
		if sm.GetLastApplied() != e.AcceptorState.InstanceID {
			t.Errorf("last applied %d, want %d",
				sm.GetLastApplied(), e.AcceptorState.InstanceID)
		}
		if storeCount == store.(*tests.KVTest).Count {
			t.Errorf("entry not applied")
		}
	}
	runSMTest2(t, tf)
}
