package paxos

import (
	"reflect"
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

func TestLogDBLastInstanceID(t *testing.T) {
	ents := []paxospb.Entry{
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
	}
	s := &TestLogDB{
		entries:          ents[1:],
		markerInstanceID: ents[0].AcceptorState.InstanceID,
	}
	_, last := s.GetRange()
	if last != 5 {
		t.Errorf("last instance id = %d, want %d", last, 5)
	}
	plog.Infof("going to append")
	s.Append([]paxospb.Entry{paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 6}}})
	_, last = s.GetRange()
	if last != 6 {
		t.Errorf("last instance id = %d, want %d", last, 6)
	}
}

func TestLogDBFirstInstanceID(t *testing.T) {
	ents := []paxospb.Entry{
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
	}
	s := &TestLogDB{
		entries:          ents[1:],
		markerInstanceID: ents[0].AcceptorState.InstanceID,
	}
	first, _ := s.GetRange()
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}
	s.Compact(4)
	first, _ = s.GetRange()
	if first != 5 {
		t.Errorf("first = %d, want %d", first, 5)
	}
}

func TestLogDBCompact(t *testing.T) {
	ents := []paxospb.Entry{
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
	}
	tests := []struct {
		i uint64

		werr        error
		winstanceID uint64
		wlen        int
	}{
		{2, ErrCompacted, 3, 3},
		{3, ErrCompacted, 3, 3},
		{4, nil, 4, 2},
		{5, nil, 5, 1},
	}
	for i, tt := range tests {
		v := make([]paxospb.Entry, len(ents))
		copy(v, ents)
		s := &TestLogDB{
			markerInstanceID: v[0].AcceptorState.InstanceID,
			entries:          v[1:],
		}
		err := s.Compact(tt.i)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if s.markerInstanceID != tt.winstanceID {
			t.Errorf("#%d: index = %d, want %d", i, s.markerInstanceID, tt.winstanceID)
		}
		if len(s.entries)+1 != tt.wlen {
			t.Errorf("#%d: len = %d, want %d", i, len(s.entries), tt.wlen)
		}
	}
}

func TestLogDBAppend(t *testing.T) {
	ents := []paxospb.Entry{
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
		paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
	}
	tests := []struct {
		entries []paxospb.Entry

		werr     error
		wentries []paxospb.Entry
	}{
		{
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
			},
			nil,
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
			},
		},
		{
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 6}},
			},
			nil,
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 6}},
			},
		},
		{
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 2}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
			},
			nil,
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
			},
		},
		{
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
			},
			nil,
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
			},
		},
		{
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 6}},
			},
			nil,
			[]paxospb.Entry{
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 3}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 4}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 5}},
				paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: 6}},
			},
		},
	}
	for i, tt := range tests {
		v := make([]paxospb.Entry, len(ents))
		copy(v, ents)
		s := &TestLogDB{
			markerInstanceID: v[0].AcceptorState.InstanceID,
			entries:          v[1:],
		}
		plog.Infof("appending $%d", i)
		err := s.Append(tt.entries)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(s.entries, tt.wentries[1:]) {
			t.Errorf("#%d: entries = %v, want %v", i, s.entries, tt.wentries[1:])
		}
	}
}
