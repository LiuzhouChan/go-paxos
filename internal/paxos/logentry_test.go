package paxos

import (
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

func getTestEntryLog() *entryLog {
	logdb := NewTestLogDB()
	return newEntryLog(logdb)
}

func TestEntryLogCanBeCreated(t *testing.T) {
	logdb := NewTestLogDB()
	logdb.Append([]paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
	})
	first, last := logdb.GetRange()
	if first != 1 || last != 3 {
		t.Error("unexpected range")
	}
	el := newEntryLog(logdb)
	if el.committed != 3 || el.applied != 0 || el.inmem.markerInstanceID != 4 {
		t.Errorf("unexpected log state %+v", el)
	}
}

func TestLogIterateOnReadyToBeAppliedEntries(t *testing.T) {
	ents := make([]paxospb.Entry, 0)
	for i := uint64(0); i <= 128; i++ {
		ents = append(ents, paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: i}})
	}
	logdb := NewTestLogDB()
	logdb.Append(ents)
	el := newEntryLog(logdb)
	el.committed = 128
	el.applied = 0
	results := make([]paxospb.Entry, 0)
	count := 0
	for {
		re := el.getEntriesToApply()
		if len(re) == 0 {
			break
		}
		count++
		results = append(results, re...)
		el.applied = re[len(re)-1].AcceptorState.InstanceID
	}
	if len(results) != 128 {
		t.Errorf("failed to get all entries")
	}
	for idx, e := range results {
		if e.AcceptorState.InstanceID != uint64(idx+1) {
			t.Errorf("unexpected instance id")
		}
	}
	if count != 1 {
		t.Errorf("unexpected count %d, want 7", count)
	}
}

func TestLogLastIndexReturnInMemLastIndexWhenPossible(t *testing.T) {
	el := getTestEntryLog()
	el.inmem.merge([]paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
	})
	if el.lastInstanceID() != 4 {
		t.Errorf("unexpected last index %d", el.lastInstanceID())
	}
}

func TestLogLastIndexReturnLogDBLastIndexWhenNothingInInMem(t *testing.T) {
	logdb := NewTestLogDB()
	logdb.Append([]paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
	})
	el := newEntryLog(logdb)
	if el.lastInstanceID() != 2 {
		t.Errorf("unexpected last index %d", el.lastInstanceID())
	}
}

func TestLogAppend(t *testing.T) {
	el := getTestEntryLog()
	el.append([]paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
	})
	ets := el.entriesToSave()
	if len(ets) != 4 {
		t.Errorf("unexpected length %d, wants 4", len(ets))
	}
}

func TestLogAppendPanicWhenAppendingCommittedEntry(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	el := getTestEntryLog()
	el.committed = 2
	el.append([]paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
	})
}

func TestLogGetEntryFromInMem(t *testing.T) {
	el := getTestEntryLog()
	el.append([]paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
	})
	ents, err := el.getEntries(1, 5)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 4 {
		t.Errorf("unexpected length %d", len(ents))
	}
	ents, err = el.getEntries(2, 4)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 2 {
		t.Errorf("unexpected length %d", len(ents))
	}
}

func TestLogGetEntryFromLogDB(t *testing.T) {
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
	}
	logdb := NewTestLogDB()
	logdb.Append(ents)
	el := newEntryLog(logdb)
	ents, err := el.getEntries(1, 5)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 4 {
		t.Errorf("unexpected length %d", len(ents))
	}
	ents, err = el.getEntries(2, 4)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 2 {
		t.Errorf("unexpected length %d", len(ents))
	}
}

func TestLogGetEntryFromLogDBAndInMem(t *testing.T) {
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
	}
	logdb := NewTestLogDB()
	logdb.Append(ents)
	el := newEntryLog(logdb)
	el.append([]paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 6}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 7}},
	})
	ents, err := el.getEntries(1, 8)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 7 {
		t.Errorf("unexpected length %d", len(ents))
	}
	ents, err = el.getEntries(2, 7)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 5 {
		t.Errorf("unexpected length %d", len(ents))
	}
	if ents[4].AcceptorState.InstanceID != 6 || ents[0].AcceptorState.InstanceID != 2 {
		t.Errorf("unexpected index")
	}
	ents, err = el.getEntries(1, 5)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if len(ents) != 4 {
		t.Errorf("unexpected length %d", len(ents))
	}
	if ents[0].AcceptorState.InstanceID != 1 || ents[3].AcceptorState.InstanceID != 4 {
		t.Errorf("unexpected index")
	}
	// entries() wrapper
	ents, err = el.entries(2)
	if err != nil {
		t.Errorf("entries failed %v", err)
	}
	if len(ents) != 6 {
		t.Errorf("unexpected length")
	}
}

func TestLogCommitTo(t *testing.T) {
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
	}
	logdb := NewTestLogDB()
	logdb.Append(ents)
	el := newEntryLog(logdb)
	el.append([]paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 6}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 7}},
	})
	el.commitTo(3)
	if el.committed != 4 {
		t.Errorf("commitedTo failed")
	}
	el.commitTo(2)
	if el.committed != 4 {
		t.Errorf("commitedTo failed")
	}
	el.commitTo(6)
	if el.committed != 6 {
		t.Errorf("commitedTo failed")
	}
}

func TestLogCommitToPanicWhenCommitToUnavailableIndex(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
	}
	logdb := NewTestLogDB()
	logdb.Append(ents)
	el := newEntryLog(logdb)
	el.append([]paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 6}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 7}},
	})
	el.commitTo(8)
}

func TestLogCommitUpdateSetsApplied(t *testing.T) {
	el := getTestEntryLog()
	el.committed = 10
	cu := paxospb.UpdateCommit{
		AppliedTo: 5,
	}
	el.commitUpdate(cu)
	if el.applied != 5 {
		t.Errorf("applied %d, want 5", el.applied)
	}
}

func TestLogCommitUpdatePanicWhenApplyTwice(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("didn't panic")
		}
	}()
	el := getTestEntryLog()
	el.applied = 6
	el.committed = 10
	cu := paxospb.UpdateCommit{
		AppliedTo: 5,
	}
	el.commitUpdate(cu)
}

func TestLogCommitUpdatePanicWhenApplyingNotCommitEntry(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("didn't panic")
		}
	}()
	el := getTestEntryLog()
	el.applied = 6
	el.committed = 10
	cu := paxospb.UpdateCommit{
		AppliedTo: 12,
	}
	el.commitUpdate(cu)
}
