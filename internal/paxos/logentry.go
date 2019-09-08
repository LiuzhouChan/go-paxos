package paxos

import (
	"errors"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

// ErrCompacted is the error returned to indicate that the requested entries
// are no longer in the LogDB due to compaction.
var ErrCompacted = errors.New("entry compacted")

// ErrUnavailable is the error returned to indicate that requested entries are
// not available in LogDB.
var ErrUnavailable = errors.New("entry unavailable")

// ILogDB is a read-only interface to the underlying persistent storage to
// allow the paxos package to access paxos state, entries, snapshots stored in
// the persistent storage. Entries stored in the persistent storage accessible
// via ILogDB is usually not required in normal cases.
type ILogDB interface {
	// GetRange returns the range of the entries in LogDB.
	GetRange() (uint64, uint64)

	// SetRange updates the ILogDB to extend the entry range known to the ILogDB.s
	SetRange(index uint64, length uint64)

	// NodeState returns the state of the node persistented in LogDB.
	NodeState() paxospb.State

	SetState(ps paxospb.State)

	Entries(low uint64, high uint64) ([]paxospb.Entry, error)

	// Append makes the given entries known to the ILogDB instance. This is
	// usually not how entries are persisted.
	Append(entries []paxospb.Entry) error
	Compact(instanceID uint64) error
}

type entryLog struct {
	logdb     ILogDB
	inmem     inMemory
	committed uint64
	applied   uint64
}

func newEntryLog(logdb ILogDB) *entryLog {
	firstInstanceID, lastInstanceID := logdb.GetRange()
	l := &entryLog{
		logdb:     logdb,
		inmem:     newInMemory(lastInstanceID),
		committed: lastInstanceID,
		applied:   firstInstanceID - 1,
	}
	return l
}

func (l *entryLog) firstInstanceID() uint64 {
	first, _ := l.logdb.GetRange()
	return first
}

func (l *entryLog) lastInstanceID() uint64 {
	instanceID, ok := l.inmem.getLastInstanceID()
	if ok {
		return instanceID
	}
	_, last := l.logdb.GetRange()
	return last
}

func (l *entryLog) entryRange() (uint64, uint64) {
	return l.firstInstanceID(), l.lastInstanceID()
}

func (l *entryLog) checkBound(low, high uint64) {
	if low > high {
		plog.Panicf("input low %d > high %d", low, high)
	}
	first, last := l.entryRange()
	if low < first || high > last+1 {
		plog.Panicf("request range[%d, %d) is out of bound [%d, %d]", low, high, first, last)
	}
}

func (l *entryLog) getEntriesFromLogDB(low, high uint64) ([]paxospb.Entry, bool, error) {
	if low >= l.inmem.markerInstanceID {
		return nil, true, nil
	}
	upperBound := min(high, l.inmem.markerInstanceID)

	ents, err := l.logdb.Entries(low, upperBound)
	if err != nil {
		panic(err)
	}
	if uint64(len(ents)) > upperBound-low {
		plog.Panicf("uint64(ents) > high-low")
	}
	return ents, uint64(len(ents)) == upperBound-low, nil
}

func (l *entryLog) getEntriesFromInMem(ents []paxospb.Entry, low uint64,
	high uint64) []paxospb.Entry {
	if high < l.inmem.markerInstanceID {
		return ents
	}
	lowBound := max(low, l.inmem.markerInstanceID)
	inmem := l.inmem.getEntries(lowBound, high)
	if len(inmem) > 0 {
		if len(ents) > 0 {
			checkEntriesToAppend(ents, inmem)
			return append(ents, inmem...)
		}
		return inmem
	}
	return ents
}

func (l *entryLog) getEntries(low, high uint64) ([]paxospb.Entry, error) {
	l.checkBound(low, high)
	if low == high {
		return nil, nil
	}
	ents, checkInMem, err := l.getEntriesFromLogDB(low, high)
	if err != nil {
		return nil, err
	}
	if !checkInMem {
		return ents, nil
	}
	return l.getEntriesFromInMem(ents, low, high), nil
}

func (l *entryLog) entries(start uint64) ([]paxospb.Entry, error) {
	if start > l.lastInstanceID() {
		return nil, nil
	}
	return l.getEntries(start, l.lastInstanceID()+1)
}

func (l *entryLog) firstNotAppliedInstanceID() uint64 {
	return max(l.applied+1, l.firstInstanceID())
}

func (l *entryLog) toApplyInstanceIDLimit() uint64 {
	return l.committed + 1
}

func (l *entryLog) hasEntriesToApply() bool {
	return l.toApplyInstanceIDLimit() > l.firstNotAppliedInstanceID()
}

func (l *entryLog) hasMoreEntriesToApply(appliedTo uint64) bool {
	return l.committed > appliedTo
}

func (l *entryLog) getEntriesToApply() []paxospb.Entry {
	if l.hasEntriesToApply() {
		if ents, err := l.getEntries(l.firstNotAppliedInstanceID(), l.toApplyInstanceIDLimit()); err != nil {
			panic(err)
		} else {
			return ents
		}
	}
	return nil
}

func (l *entryLog) entriesToSave() []paxospb.Entry {
	return l.inmem.entriesToSave()
}

func (l *entryLog) append(entries []paxospb.Entry) {
	if len(entries) == 0 {
		return
	}
	if entries[0].AcceptorState.InstanceID <= l.committed {
		plog.Panicf("commiteed entries being changed, committed %d, first instance id %d",
			l.committed, entries[0].AcceptorState.InstanceID)
	}
	l.inmem.merge(entries)
}

func (l *entryLog) commitTo(instanceID uint64) {
	if instanceID <= l.committed {
		return
	}
	if instanceID > l.lastInstanceID() {
		plog.Panicf("invalid commitTo instance id %d, last instance id %d", instanceID, l.lastInstanceID())
	}
	l.committed = instanceID
}

func (l *entryLog) commitUpdate(cu paxospb.UpdateCommit) {
	l.inmem.commitUpdate(cu)
	if cu.AppliedTo > 0 {
		if cu.AppliedTo < l.applied || cu.AppliedTo > l.committed {
			plog.Panicf("invalid applyto %d, current applied %d, commited %d", cu.AppliedTo, l.applied, l.committed)
		}
		l.applied = cu.AppliedTo
		l.inmem.appliedLogTo(cu.AppliedTo)
	}
}

func (l *entryLog) tryCommit(instanceID uint64) bool {
	if instanceID <= l.committed {
		return false
	}
	if instanceID > l.committed {
		l.commitTo(instanceID)
		return true
	}
	return false
}
