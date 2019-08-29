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
}

type entryLog struct {
	logdb     ILogDB
	committed uint64
	applied   uint64
}

func newEntryLog(logdb ILogDB) *entryLog {
	firstInstanceID, lastInstanceID := logdb.GetRange()
	l := &entryLog{
		logdb:     logdb,
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

func (l *entryLog) getEntriesFromLogDB(low, high uint64) ([]paxospb.Entry, error) {
	ents, err := l.logdb.Entries(low, high)
	if err != nil {
		panic(err)
	}
	if uint64(len(ents)) > high-low {
		plog.Panicf("uint64(ents) > high-low")
	}
	return ents, nil
}

func (l *entryLog) getEntries(low, high uint64) ([]paxospb.Entry, error) {
	l.checkBound(low, high)
	if low == high {
		return nil, nil
	}
	return l.getEntriesFromLogDB(low, high)
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
	if cu.AppliedTo > 0 {
		if cu.AppliedTo < l.applied || cu.AppliedTo > l.committed {
			plog.Panicf("invalid applyto %d, current applied %d, commited %d", cu.AppliedTo, l.applied, l.committed)
		}
		l.applied = cu.AppliedTo
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
