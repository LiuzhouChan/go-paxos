package logdb

import (
	"sync"

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
	markerInstanceID uint64
	length           uint64
	acceptorState    paxospb.AcceptorState
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
