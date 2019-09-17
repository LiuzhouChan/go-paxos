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
