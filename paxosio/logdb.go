package paxosio

import (
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

//NodeInfo ...
type NodeInfo struct {
	GroupID uint64
	NodeID  uint64
}

//GetNodeInfo ...
func GetNodeInfo(gid uint64, nid uint64) NodeInfo {
	return NodeInfo{GroupID: gid, NodeID: nid}
}

//PaxosState ...
type PaxosState struct {
	State paxospb.AcceptorState
	// FirstIndex is the index of the first entry to iterate
	FirstIndex uint64
	// EntryCount is the number of entries to iterate
	EntryCount uint64
}

// IReusableKey is the interface for keys that can be reused. A reusable key is
// usually obtained by calling the GetKey() function of the IContext
// instance.
type IReusableKey interface {
	SetEntryBatchKey(groupID uint64, nodeID uint64, index uint64)
	// SetEntryKey sets the key to be an entry key for the specified paxos node
	// with the specified entry index.
	SetEntryKey(clustergroupIDID uint64, nodeID uint64, index uint64)
	// SetStateKey sets the key to be an persistent state key suitable
	// for the specified paxos group node.
	SetStateKey(groupID uint64, nodeID uint64)
	// SetMaxIndexKey sets the key to be the max possible index key for the
	// specified paxos group node.
	SetMaxIndexKey(groupID uint64, nodeID uint64)
	// Key returns the underlying byte slice of the key.
	Key() []byte
	// Release releases the key instance so it can be reused in the future.
	Release()
}

// IContext is the per thread context used in the logdb module.
// IContext is expected to contain a list of reusable keys and byte
// slices that are owned per thread so they can be safely reused by the same
// thread when accessing ILogDB.
type IContext interface {
	Destroy()
	Reset()
	GetKey() IReusableKey
	GetValueBuffer(sz uint64) []byte
	GetUpdate() paxospb.Update
	GetWriteBatch() interface{}
	GetEntryBatch() paxospb.EntryBatch
	GetLastEntryBatch() paxospb.EntryBatch
}

// ILogDB is the interface implemented by the log DB for persistently store
// Paxos states, log entries and other Paxos metadata.
type ILogDB interface {
	Name() string
	Close()
	GetLogDBThreadContext() IContext
	ListNodeInfo() ([]NodeInfo, error)
	SavePaxosState(updates []paxospb.Update, ctx IContext) error

	// IterateEntries returns the continuous Paxos log entries of the specified
	// Paxos node between the index value range of [low, high) up to a max size
	// limit of maxSize bytes. It returns the located log entries, their total
	// size in bytes and the occurred error.
	IterateEntries(ents []paxospb.Entry,
		size uint64, groupID uint64, nodeID uint64, low uint64,
		high uint64, maxSize uint64) ([]paxospb.Entry, uint64, error)

	//ReadPaxosState returns the persistented paxos state found in Log DB.
	ReadPaxosState(groupID, nodeID, lastInstance uint64) (*PaxosState, error)

	// RemoveEntriesTo removes entries associated with the specified paxos node up
	// to the specified instance.
	RemoveEntriesTo(groupID, nodeID, instance uint64) error

	SaveSnapshot(groupID, nodeID, instance uint64) error
}
