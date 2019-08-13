package rsm

import (
	"errors"
	"sync"

	"github.com/LiuzhouChan/go-paxos/paxospb"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/logger"
)

var (
	plog = logger.GetLogger("rsm")
)

var (
	// ErrSaveSnapshot indicates there is error when trying to save a snapshot
	ErrSaveSnapshot = errors.New("failed to save snapshot")
	// ErrRestoreSnapshot indicates there is error when trying to restore
	// from a snapshot
	ErrRestoreSnapshot      = errors.New("failed to restore from snapshot")
	commitChanLength        = settings.Soft.NodeCommitChanLength
	commitChanBusyThreshold = settings.Soft.NodeCommitChanLength / 2
	batchedEntryApply       = settings.Soft.BatchedEntryApply > 0
)

// Commit is the processing units that can be handled by statemache
type Commit struct {
	InstanceID        uint64
	SnapshotAvailable bool
	InitialSnapshot   bool
	SnapshotRequested bool
	Entries           []paxospb.Entry
}

// SMFactoryFunc is the function type for creating an IStateMachine instance
type SMFactoryFunc func(groupID uint64,
	nodeID uint64, done <-chan struct{}) IManagedStateMachine

// INodeProxy is the interface used as proxy to a nodehost.
type INodeProxy interface {
	ApplyUpdate(paxospb.Entry, uint64, bool, bool, bool)
	NodeID() uint64
	ClusterID() uint64
}

// StateMachine is a manager class that manages application state
// machine
type StateMachine struct {
	mu          sync.RWMutex
	node        INodeProxy
	sm          IManagedStateMachine
	lastApplied uint64
}
