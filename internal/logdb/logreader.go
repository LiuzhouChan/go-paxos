package logdb

import (
	"fmt"
	"sync"

	ipaxos "github.com/LiuzhouChan/go-paxos/internal/paxos"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

// LogReader is the struct used to manage logs that have already been persisted
// into LogDB. This implementation is influenced by CockroachDB's
// replicaRaftStorage.
type LogReader struct {
	sync.Mutex
	groupID          uint64
	nodeID           uint64
	logdb            paxosio.ILogDB
	state            paxospb.State
	markerInstanceID uint64
	length           uint64
}

// NewLogReader creates and returns a new LogReader instance.
func NewLogReader(groupID uint64, nodeID uint64, logdb paxosio.ILogDB) *LogReader {
	return &LogReader{
		logdb:   logdb,
		groupID: groupID,
		nodeID:  nodeID,
		length:  1,
	}
}

//NodeState ...
func (lr *LogReader) NodeState() paxospb.State {
	lr.Lock()
	defer lr.Unlock()
	return lr.state
}

//Entries ...
func (lr *LogReader) Entries(low, high uint64) ([]paxospb.Entry, error) {
	lr.Lock()
	defer lr.Unlock()
	return lr.entriesLocked(low, high)
}

func (lr *LogReader) entriesLocked(low, high uint64) ([]paxospb.Entry, error) {
	if low > high {
		return nil, fmt.Errorf("high %d < low %d", high, low)
	}
	if low <= lr.markerInstanceID {
		return nil, ipaxos.ErrCompacted
	}
	if high > lr.lastInstanceID()+1 {
		plog.Errorf("low %d high %d, last instance id %d", low, high, lr.lastInstanceID())
		return nil, ipaxos.ErrUnavailable
	}
	ents, err := lr.logdb.IterateEntries(lr.groupID, lr.nodeID, low, high)
	if err != nil {
		return nil, err
	}
	if uint64(len(ents)) == high-low {
		return ents, nil
	}
	plog.Warningf("failed to get anything from logreader")
	return nil, ipaxos.ErrUnavailable
}

// GetRange returns the index range of all logs managed by the LogReader
// instance.
func (lr *LogReader) GetRange() (uint64, uint64) {
	lr.Lock()
	defer lr.Unlock()
	return lr.firstInstanceID(), lr.lastInstanceID()
}

func (lr *LogReader) firstInstanceID() uint64 {
	return lr.markerInstanceID + 1
}

func (lr *LogReader) lastInstanceID() uint64 {
	return lr.markerInstanceID + lr.length - 1
}

// SetRange updates the LogReader to reflect what is available in it.
func (lr *LogReader) SetRange(firstInstanceID, length uint64) {
	if length == 0 {
		return
	}
	lr.Lock()
	defer lr.Unlock()
	first := lr.firstInstanceID()
	last := firstInstanceID + length - 1
	if last < first {
		return
	}
	if first > firstInstanceID {
		cut := first - firstInstanceID
		firstInstanceID = first
		length -= cut
	}
	offset := firstInstanceID - lr.markerInstanceID
	switch {
	case lr.length > offset:
		lr.length = offset + length
	case lr.length == offset:
		lr.length = lr.length + length
	default:
		panic("missing log entry")
	}
}

// SetState sets the persistent state.
func (lr *LogReader) SetState(s paxospb.State) {
	lr.Lock()
	defer lr.Unlock()
	lr.state = s
}
