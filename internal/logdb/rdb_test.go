package logdb

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const (
	RDBTestDirectory = "rdb_test_dir_safe_to_delete"
)

func getNewTestDB(dir string, lldir string) paxosio.ILogDB {
	d := filepath.Join(RDBTestDirectory, dir)
	lld := filepath.Join(RDBTestDirectory, lldir)
	os.MkdirAll(d, 0777)
	os.MkdirAll(lld, 0777)
	db, err := OpenLogDB([]string{d}, []string{lld})
	if err != nil {
		panic(err)
	}
	return db
}

func deleteTestDB() {
	os.RemoveAll(RDBTestDirectory)
}

func runLogDBTest(t *testing.T, tf func(t *testing.T, db paxosio.ILogDB)) {
	defer leaktest.AfterTest(t)()
	dir := "db-dir"
	lldir := "wal-db-dir"
	db := getNewTestDB(dir, lldir)
	defer deleteTestDB()
	defer db.Close()
	tf(t, db)
}

func TestRDBReturnErrNoBootstrapInfoWhenNoBootstrap(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		bootstrap, err := db.GetBootstrapInfo(1, 2)
		if err != paxosio.ErrNoBootstrapInfo {
			t.Errorf("unexpected error %v", err)
		}
		if bootstrap != nil {
			t.Errorf("not nil value")
		}
	}
	runLogDBTest(t, tf)
}

func TestBootstrapInfoCanBeSaveAndChecked(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		nodes := make(map[uint64]string)
		nodes[100] = "address1"
		nodes[200] = "address2"
		nodes[300] = "address3"
		bs := paxospb.Bootstrap{
			Join:      false,
			Addresses: nodes,
		}
		if err := db.SaveBootstrapInfo(1, 2, bs); err != nil {
			t.Errorf("failed to save bootstrap info %v", err)
		}
		bootstrap, err := db.GetBootstrapInfo(1, 2)
		if err != nil {
			t.Errorf("failed to get bootstrap info %v", err)
		}
		if bootstrap.Join {
			t.Errorf("unexpected join value")
		}
		if len(bootstrap.Addresses) != 3 {
			t.Errorf("unexpected addresses len")
		}
		ni, err := db.ListNodeInfo()
		if err != nil {
			t.Errorf("failed to list node info %v", err)
		}
		if len(ni) != 1 {
			t.Errorf("failed to get node info list")
		}
		if ni[0].GroupID != 1 || ni[0].NodeID != 2 {
			t.Errorf("unexpected group id/node, %v", ni[0])
		}
		if err := db.SaveBootstrapInfo(2, 3, bs); err != nil {
			t.Errorf("failed to save bootstrap info %v", err)
		}
		if err := db.SaveBootstrapInfo(3, 4, bs); err != nil {
			t.Errorf("failed to save bootstrap info %v", err)
		}
		ni, err = db.ListNodeInfo()
		if err != nil {
			t.Errorf("failed to list node info %v", err)
		}
		if len(ni) != 3 {
			t.Errorf("failed to get node info list")
		}
	}
	runLogDBTest(t, tf)
}

func TestMaxInstanceIDRuleIsEnforced(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		hs := paxospb.State{
			Commit: 100,
		}
		e1 := paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			AcceptorState: paxospb.AcceptorState{
				InstanceID:    10,
				AccetpedValue: []byte("test data"),
			},
		}
		e2 := paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			AcceptorState: paxospb.AcceptorState{
				InstanceID:    3,
				AccetpedValue: []byte("test data 2"),
			},
		}
		ud := paxospb.Update{
			EntriesToSave: []paxospb.Entry{e1, e2},
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err := db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		ud = paxospb.Update{
			EntriesToSave: []paxospb.Entry{e2},
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err = db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err := db.ReadPaxosState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to read")
		}
		if rs.EntryCount != 1 {
			t.Errorf("entry sz %d, want 1", rs.EntryCount)
			return
		}
		if rs.FirstInstanceID != 3 {
			t.Errorf("entry instanceID %d, want 3", rs.FirstInstanceID)
		}
	}
	runLogDBTest(t, tf)
}

func TestSavedEntrieseAreOrderedByTheKey(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		hs := paxospb.State{
			Commit: 100,
		}
		ents := make([]paxospb.Entry, 0)
		for i := uint64(0); i < 1024; i++ {
			e := paxospb.Entry{
				Type: paxospb.ApplicationEntry,
				AcceptorState: paxospb.AcceptorState{
					InstanceID:    i,
					AccetpedValue: []byte("test data"),
				},
			}
			ents = append(ents, e)
		}
		ud := paxospb.Update{
			EntriesToSave: ents,
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err := db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err := db.ReadPaxosState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to read")
		}
		if rs.EntryCount != 1024 {
			t.Errorf("entries size %d, want %d", rs.EntryCount, 1024)
		}
		re, err := db.IterateEntries(3, 4, 0, math.MaxUint64)
		if err != nil {
			t.Errorf("IterateEntries failed %v", err)
		}
		if len(re) != 1024 {
			t.Errorf("didn't return all entries")
		}
		lastInstancID := re[0].AcceptorState.InstanceID
		for _, e := range re[1:] {
			if e.AcceptorState.InstanceID != lastInstancID+1 {
				t.Errorf("instance id not sequential")
			}
			lastInstancID = e.AcceptorState.InstanceID
		}
	}
	runLogDBTest(t, tf)
}

func testSavePaxosState(t *testing.T, db paxosio.ILogDB) {
	hs := paxospb.State{
		Commit: 100,
	}
	ud := paxospb.Update{
		State:   hs,
		GroupID: 3,
		NodeID:  4,
	}
	for i := uint64(1); i <= 10; i++ {
		e := paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			Key:  1234,
			AcceptorState: paxospb.AcceptorState{
				InstanceID:    i,
				AccetpedValue: []byte("test data"),
			},
		}
		ud.EntriesToSave = append(ud.EntriesToSave, e)
	}
	err := db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
	if err != nil {
		t.Errorf("failed to save single de rec")
	}
	rs, err := db.ReadPaxosState(3, 4, 0)
	if err != nil {
		t.Errorf("failed to read")
	}
	if rs.State == nil {
		t.Errorf("failed to get hs")
	}
	if rs.State.Commit != 100 {
		t.Errorf("bad hs returned value")
	}
	if rs.EntryCount != 10 {
		t.Errorf("didn't return all entries")
	}
}

func TestSavePaxosState(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		testSavePaxosState(t, db)
	}
	runLogDBTest(t, tf)
}

func TestStateIsUpdated(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		hs := paxospb.State{
			Commit: 100,
		}
		ud := paxospb.Update{
			EntriesToSave: []paxospb.Entry{},
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err := db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err := db.ReadPaxosState(3, 4, 0)
		if err != nil {
			t.Errorf("read paxos state failed %v", err)
		}
		if rs.State.Commit != 100 {
			t.Errorf("unexpected persistent state value %v", rs)
		}
		hs2 := paxospb.State{
			Commit: 101,
		}
		ud2 := paxospb.Update{
			EntriesToSave: []paxospb.Entry{},
			State:         hs2,
			GroupID:       3,
			NodeID:        4,
		}
		err = db.SavePaxosState([]paxospb.Update{ud2}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("save paxos state failed %v", err)
		}
		rs, err = db.ReadPaxosState(3, 4, 0)
		if err != nil {
			t.Errorf("read paxos state failed %v", err)
		}
		if rs.State.Commit != 101 {
			t.Errorf("2 unexpected persistent state value %v", rs)
		}
	}
	runLogDBTest(t, tf)
}

func TestMaxInstanceIDIsUpdate(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		hs := paxospb.State{
			Commit: 100,
		}
		e1 := paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			Key:  1234,
			AcceptorState: paxospb.AcceptorState{
				InstanceID:    10,
				AccetpedValue: []byte("test data"),
			},
		}
		ud := paxospb.Update{
			EntriesToSave: []paxospb.Entry{e1},
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err := db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		p := db.(*ShardedRDB).shards
		maxInstanceID, err := p[3].readMaxInstance(3, 4)
		if err != nil {
			t.Errorf("%v", err)
		}
		if maxInstanceID != 10 {
			t.Errorf("max instance id %d, want 10", maxInstanceID)
		}
		e1 = paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			Key:  1235,
			AcceptorState: paxospb.AcceptorState{
				InstanceID:    11,
				AccetpedValue: []byte("test data"),
			},
		}
		ud = paxospb.Update{
			EntriesToSave: []paxospb.Entry{e1},
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err = db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		maxInstanceID, err = p[3].readMaxInstance(3, 4)
		if err != nil {
			t.Errorf("%v", err)
		}
		if maxInstanceID != 11 {
			t.Errorf("max instance id %d, want 11", maxInstanceID)
		}
	}
	runLogDBTest(t, tf)
}

func TestReadAllEntriesOnlyReturnEntriesFromTheSpecifiedNode(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		hs := paxospb.State{
			Commit: 100,
		}
		e1 := paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			Key:  1234,
			AcceptorState: paxospb.AcceptorState{
				InstanceID:    10,
				AccetpedValue: []byte("test data"),
			},
		}
		e2 := e1
		e2.AcceptorState.InstanceID = 11
		e2.AcceptorState.AccetpedValue = []byte("test data 2")
		ud := paxospb.Update{
			EntriesToSave: []paxospb.Entry{e1, e2},
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err := db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err := db.ReadPaxosState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to get the entries %v", err)
		}
		if rs.EntryCount != 2 {
			t.Errorf("ents sz %d, want 2", rs.EntryCount)
		}
		// save the same data but with different node id
		ud.NodeID = 5
		err = db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err = db.ReadPaxosState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to get the entries %v", err)
		}
		if rs.EntryCount != 2 {
			t.Errorf("ents sz %d, want 2", rs.EntryCount)
		}
		ud.NodeID = 4
		ud.GroupID = 4
		err = db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		rs, err = db.ReadPaxosState(3, 4, 0)
		if err != nil {
			t.Errorf("failed to get the entries %v", err)
		}
		if rs.EntryCount != 2 {
			t.Errorf("ents sz %d, want 2", rs.EntryCount)
		}
	}
	runLogDBTest(t, tf)
}

func TestIterateEntriesOnlyReturnCurrentNodeEntries(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		ents, _ := db.IterateEntries(3, 4, 10, 13)
		if len(ents) != 0 {
			t.Errorf("ents sz %d, want 0", len(ents))
		}
		hs := paxospb.State{
			Commit: 100,
		}
		e1 := paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			Key:  1234,
			AcceptorState: paxospb.AcceptorState{
				InstanceID:    10,
				AccetpedValue: []byte("test data 1"),
			},
		}
		e2 := e1
		e2.AcceptorState.InstanceID = 11
		e2.AcceptorState.AccetpedValue = []byte("test data 2")
		e3 := e1
		e3.AcceptorState.InstanceID = 12
		e3.AcceptorState.AccetpedValue = []byte("test data 3")

		ud := paxospb.Update{
			EntriesToSave: []paxospb.Entry{e1, e2, e3},
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err := db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		ud.NodeID = 5
		err = db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save updated de rec")
		}
		ents, _ = db.IterateEntries(3, 4, 10, 13)
		if len(ents) != 3 {
			t.Errorf("ents sz %d, want 3", len(ents))
		}
		ud.NodeID = 4
		ud.GroupID = 4
		err = db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save updated de rec")
		}

		ents, _ = db.IterateEntries(3, 4, 10, 13)
		if len(ents) != 3 {
			t.Errorf("ents sz %d, want 3", len(ents))
		}
	}
	runLogDBTest(t, tf)
}

func TestIterateEntries(t *testing.T) {
	tf := func(t *testing.T, db paxosio.ILogDB) {
		ents, _ := db.IterateEntries(3, 4, 10, 13)
		if len(ents) != 0 {
			t.Errorf("ents sz %d, want 0", len(ents))
		}
		hs := paxospb.State{
			Commit: 100,
		}
		e1 := paxospb.Entry{
			Type: paxospb.ApplicationEntry,
			Key:  1234,
			AcceptorState: paxospb.AcceptorState{
				InstanceID:    10,
				AccetpedValue: []byte("test data 1"),
			},
		}
		e2 := e1
		e2.AcceptorState.InstanceID = 11
		e2.AcceptorState.AccetpedValue = []byte("test data 2")
		e3 := e1
		e3.AcceptorState.InstanceID = 12
		e3.AcceptorState.AccetpedValue = []byte("test data 3")
		ud := paxospb.Update{
			EntriesToSave: []paxospb.Entry{e1, e2, e3},
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err := db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		ents, _ = db.IterateEntries(3, 4, 10, 13)
		if len(ents) != 3 {
			t.Errorf("ents sz %d, want 3", len(ents))
		}
		ents, _ = db.IterateEntries(3, 4, 10, 12)
		if len(ents) != 2 {
			t.Errorf("ents sz %d, want 2", len(ents))
		}
		// write an entry index 11
		ud = paxospb.Update{
			EntriesToSave: []paxospb.Entry{e2},
			State:         hs,
			GroupID:       3,
			NodeID:        4,
		}
		err = db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
		ents, _ = db.IterateEntries(3, 4, 10, 13)
		if len(ents) != 2 {
			t.Errorf("ents sz %d, want 2", len(ents))
		}
		for _, ent := range ents {
			if ent.AcceptorState.InstanceID == 12 {
				t.Errorf("instance id 12 found")
			}
		}
	}
	runLogDBTest(t, tf)
}

func TestParseNodeInfoKeyPanicOnUnexpectedKeySize(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	parseNodeInfoKey(make([]byte, 21))
}
