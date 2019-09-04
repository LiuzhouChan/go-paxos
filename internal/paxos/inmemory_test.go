package paxos

import (
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

func TestInMemCheckMarkerInstanceID(t *testing.T) {
	im := inMemory{markerInstanceID: 10}
	im.checkMarkerInstanceID()
	im = inMemory{
		entries: []paxospb.Entry{
			{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
			{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		},
		markerInstanceID: 1,
	}
	im.checkMarkerInstanceID()
}

func TestInMemCheckMarkerIndexPanicOnInvalidInMem(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	im := inMemory{
		entries: []paxospb.Entry{
			{AcceptorState: paxospb.AcceptorState{InstanceID: 1}},
			{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
		},
		markerInstanceID: 2,
	}
	im.checkMarkerInstanceID()
}

func testGetEntriesPanicWithInvalidInput(low uint64,
	high uint64, marker uint64, firstIndex uint64, length uint64, t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Errorf("panic not triggered")
	}()
	im := &inMemory{
		markerInstanceID: marker,
		entries:          make([]paxospb.Entry, 0),
	}
	for i := firstIndex; i < firstIndex+length; i++ {
		im.entries = append(im.entries, paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: i}})
	}
	im.getEntries(low, high)
}

func TestInMemGetEntriesPanicWithInvalidInput(t *testing.T) {
	tests := []struct {
		low        uint64
		high       uint64
		marker     uint64
		firstIndex uint64
		length     uint64
	}{
		{10, 9, 10, 10, 10},  // low > high
		{10, 11, 11, 10, 10}, // low < markerIndex
		{10, 11, 5, 5, 5},    // high > upperBound
	}
	for _, tt := range tests {
		testGetEntriesPanicWithInvalidInput(tt.low,
			tt.high, tt.marker, tt.firstIndex, tt.length, t)
	}
}

func TestInMemGetEntries(t *testing.T) {
	im := &inMemory{
		markerInstanceID: 2,
		entries: []paxospb.Entry{
			{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
			{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		},
	}
	ents := im.getEntries(2, 3)
	if len(ents) != 1 {
		t.Errorf("ents len %d", len(ents))
	}
	ents = im.getEntries(2, 4)
	if len(ents) != 2 {
		t.Errorf("ents len %d", len(ents))
	}
}

func TestInMemGetLastIndex(t *testing.T) {
	tests := []struct {
		first  uint64
		length uint64
	}{
		{100, 5},
		{1, 100},
	}
	for idx, tt := range tests {
		im := inMemory{
			entries: make([]paxospb.Entry, 0),
		}
		for i := tt.first; i < tt.first+tt.length; i++ {
			im.entries = append(im.entries, paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: i}})
		}
		index, ok := im.getLastIndex()
		if !ok || index != tt.first+tt.length-1 {
			t.Errorf("%d, ok %t, index %d, want %d", idx, ok, index, tt.first+tt.length-1)
		}
	}
}

func TestInMemMergeFullAppend(t *testing.T) {
	im := inMemory{
		markerInstanceID: 5,
		entries: []paxospb.Entry{
			{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
			{AcceptorState: paxospb.AcceptorState{InstanceID: 6}},
			{AcceptorState: paxospb.AcceptorState{InstanceID: 7}},
		},
	}
	ents := []paxospb.Entry{
		{AcceptorState: paxospb.AcceptorState{InstanceID: 8}},
		{AcceptorState: paxospb.AcceptorState{InstanceID: 9}},
	}
	im.merge(ents)
	if len(im.entries) != 5 || im.markerInstanceID != 5 {
		t.Errorf("not fully appended")
	}
	if idx, ok := im.getLastIndex(); !ok || idx != 9 {
		t.Errorf("last index %d, want 9", idx)
	}
}
