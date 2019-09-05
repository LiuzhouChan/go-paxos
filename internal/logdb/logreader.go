package logdb

import (
	"fmt"
	"sync"

	ipaxos "github.com/LiuzhouChan/go-paxos/internal/paxos"
	"github.com/LiuzhouChan/go-paxos/internal/utils/logutil"
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
	l := &LogReader{
		logdb:   logdb,
		groupID: groupID,
		nodeID:  nodeID,
		length:  1,
	}
	return l
}

func (lr *LogReader) describe() string {
	return fmt.Sprintf("logreader %s instance id %d length %d",
		logutil.DescribeNode(lr.groupID, lr.nodeID), lr.markerInstanceID, lr.length)
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
	if len(ents) > 0 {
		if ents[0].AcceptorState.InstanceID > low {
			return nil, ipaxos.ErrCompacted
		}
		expected := ents[len(ents)-1].AcceptorState.InstanceID + 1
		if lr.lastInstanceID() <= expected {
			plog.Errorf("%s, %v, low %d high %d, expected %d last instance id %d",
				lr.describe(), ipaxos.ErrUnavailable, low, high, expected, lr.lastInstanceID())
			return nil, ipaxos.ErrUnavailable
		}
		return nil, fmt.Errorf("gap found between [%d:%d] at %d", low, high, expected)
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

// Append ...
func (lr *LogReader) Append(entries []paxospb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if len(entries) > 0 {
		if entries[0].AcceptorState.InstanceID+uint64(len(entries))-1 !=
			entries[len(entries)-1].AcceptorState.InstanceID {
			panic("gap in entries")
		}
	}
	lr.SetRange(entries[0].AcceptorState.InstanceID, uint64(len(entries)))
	return nil
}

// Compact compacts paxos log entries up to index
func (lr *LogReader) Compact(instanceID uint64) error {
	lr.Lock()
	defer lr.Unlock()
	if instanceID < lr.markerInstanceID {
		return ipaxos.ErrCompacted
	}
	if instanceID > lr.lastInstanceID() {
		return ipaxos.ErrUnavailable
	}
	i := instanceID - lr.markerInstanceID
	lr.length = lr.length - i
	lr.markerInstanceID = instanceID
	return nil
}
