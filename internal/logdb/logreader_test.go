package logdb

import (
	"reflect"
	"testing"

	ipaxos "github.com/LiuzhouChan/go-paxos/internal/paxos"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const (
	LogReaderTestGroupID uint64 = 2
	LogReaderTestNodeID  uint64 = 12345
	defaultCacheSize     uint64 = 1024 * 1024
)

func TestLogReaderNewLogReader(t *testing.T) {
	lr := NewLogReader(1, 1, nil)
	if lr.length != 1 {
		t.Errorf("unexpected length")
	}
}

func TestInitialState(t *testing.T) {
	lr := NewLogReader(1, 1, nil)
	ps := paxospb.State{
		Commit: 123,
	}
	lr.SetState(ps)
	rps := lr.NodeState()
	if !reflect.DeepEqual(&rps, &ps) {
		t.Errorf("unexpected state")
	}
}

func TestLogReaderInstanceIDRange(t *testing.T) {
	lr := NewLogReader(1, 1, nil)
	first := lr.firstInstanceID()
	if first != 1 {
		t.Errorf("unexpected first instance id %d", first)
	}
	last := lr.lastInstanceID()
	if last != 0 {
		t.Errorf("unexpected last instance id %d", last)
	}
	fi, li := lr.GetRange()
	if fi != first || li != last {
		t.Errorf("unexpected index")
	}
}

func TestSetRange(t *testing.T) {
	tests := []struct {
		marker     uint64
		length     uint64
		instanceID uint64
		idxLength  uint64
		expLength  uint64
	}{
		{1, 10, 1, 1, 10},
		{1, 10, 1, 0, 10},
		{10, 10, 8, 10, 8},
		{10, 10, 20, 10, 20},
	}
	for idx, tt := range tests {
		lr := LogReader{
			markerInstanceID: tt.marker,
			length:           tt.length,
		}
		lr.SetRange(tt.instanceID, tt.idxLength)
		if lr.length != tt.expLength {
			t.Errorf("%d, unexpected length %d, want %d", idx, lr.length, tt.expLength)
		}
	}
}

func TestSetRangePanicWhenThereIsIndexHole(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	lr := LogReader{
		markerInstanceID: 10,
		length:           10,
	}
	lr.SetRange(100, 100)
}

func getNewLogReaderTestDB(entries []paxospb.Entry) paxosio.ILogDB {
	logdb := getNewTestDB("db-dir", "wal-db-dir")
	ud := paxospb.Update{
		EntriesToSave: entries,
		GroupID:       LogReaderTestGroupID,
		NodeID:        LogReaderTestNodeID,
	}
	if err := logdb.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil)); err != nil {
		panic(err)
	}
	return logdb
}

func getTestLogReader(entries []paxospb.Entry) *LogReader {
	logdb := getNewLogReaderTestDB(entries)
	ls := NewLogReader(LogReaderTestGroupID, LogReaderTestNodeID, logdb)
	ls.markerInstanceID = entries[0].AcceptorState.InstanceID
	ls.length = uint64(len(entries))
	return ls
}

func TestLogReaderEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testLogReaderEntries(t)
}

func testLogReaderEntries(t *testing.T) {
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 6}},
	}
	tests := []struct {
		lo, hi   uint64
		werr     error
		wentries []paxospb.Entry
	}{
		{2, 6, ipaxos.ErrCompacted, nil},
		{3, 4, ipaxos.ErrCompacted, nil},
		{4, 5, nil, []paxospb.Entry{{AcceptorState: paxospb.AcceptorState{InstanceID: 4}}}},
		{4, 6, nil, []paxospb.Entry{{AcceptorState: paxospb.AcceptorState{InstanceID: 4}}, {AcceptorState: paxospb.AcceptorState{InstanceID: 5}}}},
		{4, 7, nil, []paxospb.Entry{{AcceptorState: paxospb.AcceptorState{InstanceID: 4}}, {AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
			{AcceptorState: paxospb.AcceptorState{InstanceID: 6}}}},
	}

	for i, tt := range tests {
		s := getTestLogReader(ents)
		entries, err := s.Entries(tt.lo, tt.hi)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, entries, tt.wentries)
		}
		s.logdb.Close()
		deleteTestDB()
	}
}

func TestLogReaderLastIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testLogReaderLastIndex(t)
}

func testLogReaderLastIndex(t *testing.T) {
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
	}
	s := getTestLogReader(ents)
	_, last := s.GetRange()
	if last != 5 {
		t.Errorf("instanceID = %d, want %d", last, 5)
	}
	s.Append([]paxospb.Entry{{AcceptorState: paxospb.AcceptorState{InstanceID: 6}}})
	_, last = s.GetRange()
	if last != 6 {
		t.Errorf("last = %d, want %d", last, 5)
	}
	s.logdb.Close()
	deleteTestDB()
}

func TestLogReaderFirstIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testLogReaderFirstIndex(t)
}

func testLogReaderFirstIndex(t *testing.T) {
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
	}
	s := getTestLogReader(ents)
	first, _ := s.GetRange()
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}
	_, li := s.GetRange()
	if li != 5 {
		t.Errorf("last index = %d, want 5", li)
	}
	s.Compact(4)
	first, _ = s.GetRange()
	if first != 5 {
		t.Errorf("first = %d, want %d", first, 5)
	}
	_, li = s.GetRange()
	if li != 5 {
		t.Errorf("last index = %d, want 5", li)
	}
	s.logdb.Close()
	deleteTestDB()
}

func TestLogReaderSetRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
	}

	tests := []struct {
		firstIndex          uint64
		length              uint64
		expLength           uint64
		expMarkerInstanceID uint64
	}{
		{2, 2, 3, 3},
		{2, 5, 4, 3},
		{3, 5, 5, 3},
		{6, 6, 9, 3},
	}
	for idx, tt := range tests {
		s := getTestLogReader(ents)
		s.SetRange(tt.firstIndex, tt.length)
		if s.markerInstanceID != tt.expMarkerInstanceID {
			t.Errorf("%d, marker index %d, want %d", idx, s.markerInstanceID, tt.expMarkerInstanceID)
		}
		if s.length != tt.expLength {
			t.Errorf("%d, length %d, want %d", idx, s.length, tt.expLength)
		}
		s.logdb.Close()
		deleteTestDB()
	}
}

func TestLogReaderInitialState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
	}

	s := getTestLogReader(ents)
	defer deleteTestDB()
	defer s.logdb.Close()

	ps := paxospb.State{
		Commit: 5,
	}
	s.SetState(ps)
	rps := s.NodeState()
	if !reflect.DeepEqual(&rps, &ps) {
		t.Errorf("initial state unexpected")
	}
}
