package paxos

import "github.com/LiuzhouChan/go-paxos/paxospb"

// TestLogDB is used in paxos test only. It is basically a logdb.logreader
// backed by a []entry.
type TestLogDB struct {
	entries          []paxospb.Entry
	markerInstanceID uint64
	state            paxospb.State
}

func NewTestLogDB() ILogDB {
	return &TestLogDB{
		entries: make([]paxospb.Entry, 0),
	}
}

func (db *TestLogDB) SetState(s paxospb.State) {
	db.state = s
}

func (db *TestLogDB) NodeState() paxospb.State {
	return db.state
}

func (db *TestLogDB) GetRange() (uint64, uint64) {
	return db.firstInstanceID(), db.lastInstanceID()
}

func (db *TestLogDB) firstInstanceID() uint64 {
	return db.markerInstanceID + 1
}

func (db *TestLogDB) lastInstanceID() uint64 {
	return db.markerInstanceID + uint64(len(db.entries))
}

func (db *TestLogDB) SetRange(firstInstanceID uint64, length uint64) {
	panic("not implemented")
}

func (db *TestLogDB) Append(entries []paxospb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := db.firstInstanceID()
	if db.markerInstanceID+uint64(len(entries)) < first {
		return nil
	}
	if first > entries[0].AcceptorState.InstanceID {
		entries = entries[first-entries[0].AcceptorState.InstanceID:]
	}
	offset := entries[0].AcceptorState.InstanceID - db.markerInstanceID
	if uint64(len(db.entries)+1) > offset {
		db.entries = db.entries[:offset-1]
	} else if uint64(len(db.entries)+1) < offset {
		plog.Panicf("found a hole last index %d, first incoming index %d",
			db.lastInstanceID(), entries[0].AcceptorState.InstanceID)
	}
	db.entries = append(db.entries, entries...)
	return nil
}

func (db *TestLogDB) Entries(low uint64,
	high uint64) ([]paxospb.Entry, error) {
	if low <= db.markerInstanceID {
		return nil, ErrCompacted
	}
	if high > db.lastInstanceID()+1 {
		return nil, ErrUnavailable
	}
	if len(db.entries) == 0 {
		return nil, ErrUnavailable
	}
	ents := db.entries[low-db.markerInstanceID-1 : high-db.markerInstanceID-1]
	return ents, nil
}

func (db *TestLogDB) Compact(intanceID uint64) error {
	if intanceID <= db.markerInstanceID {
		return ErrCompacted
	}
	if intanceID > db.lastInstanceID() {
		return ErrUnavailable
	}
	if len(db.entries) == 0 {
		return ErrUnavailable
	}

	cut := intanceID - db.markerInstanceID
	db.entries = db.entries[cut:]
	db.markerInstanceID = intanceID
	return nil
}
