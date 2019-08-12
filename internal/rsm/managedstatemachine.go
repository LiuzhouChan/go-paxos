package rsm

import (
	"errors"
	"sync"

	"github.com/LiuzhouChan/go-paxos/statemachine"
)

var (
	// ErrGroupClosed indicates that the group has been closed
	ErrGroupClosed = errors.New("paxos group already closed")
)

// From identifies a component in the system.
type From uint64

const (
	// FromNodeHost indicates the data store has been loaded by or offloaded from
	// nodehost.
	FromNodeHost From = iota
	// FromStepWorker indicates that the data store has been loaded by or
	// offloaded from the step worker.
	FromStepWorker
	// FromCommitWorker indicates that the data store has been loaded by or
	// offloaded from the commit worker.
	FromCommitWorker
	// FromSnapshotWorker indicates that the data store has been loaded by or
	// offloaded from the snapshot worker.
	FromSnapshotWorker
)

// OffloadedStatus is used for tracking whether the managed data store has been
// offloaded from various system components.
type OffloadedStatus struct {
	readyToDestroy              bool
	destroyed                   bool
	offloadedFromNodeHost       bool
	offloadedFromStepWorker     bool
	offloadedFromCommitWorker   bool
	offloadedFromSnapshotWorker bool
	loadedByStepWorker          bool
	loadedByCommitWorker        bool
	loadedBySnapshotWorker      bool
}

// ReadyToDestroy returns a boolean value indicating whether the the managed data
// store is ready to be destroyed.
func (o *OffloadedStatus) ReadyToDestroy() bool {
	return o.readyToDestroy
}

// Destroyed returns a boolean value indicating whether the belonging object
// has been destroyed.
func (o *OffloadedStatus) Destroyed() bool {
	return o.destroyed
}

// SetDestroyed set the destroyed flag to be true
func (o *OffloadedStatus) SetDestroyed() {
	o.destroyed = true
}

// SetLoaded marks the managed data store as loaded from the specified
// component.
func (o *OffloadedStatus) SetLoaded(from From) {
	if o.offloadedFromNodeHost {
		if from == FromStepWorker ||
			from == FromCommitWorker ||
			from == FromSnapshotWorker {
			plog.Panicf("loaded from %v after offloaded from nodehost", from)
		}
	}
	if from == FromNodeHost {
		panic("not suppose to get loaded notification from nodehost")
	} else if from == FromStepWorker {
		o.loadedByStepWorker = true
	} else if from == FromCommitWorker {
		o.loadedByCommitWorker = true
	} else if from == FromSnapshotWorker {
		o.loadedBySnapshotWorker = true
	} else {
		panic("unknown offloadFrom value")
	}
}

// SetOffloaded marks the managed data store as offloaded from the specified
// component.
func (o *OffloadedStatus) SetOffloaded(from From) {
	if from == FromNodeHost {
		o.offloadedFromNodeHost = true
	} else if from == FromStepWorker {
		o.offloadedFromStepWorker = true
	} else if from == FromCommitWorker {
		o.offloadedFromCommitWorker = true
	} else if from == FromSnapshotWorker {
		o.offloadedFromSnapshotWorker = true
	} else {
		panic("unknown offloadFrom value")
	}
	if from == FromNodeHost {
		if !o.loadedByStepWorker {
			o.offloadedFromStepWorker = true
		}
		if !o.loadedByCommitWorker {
			o.offloadedFromCommitWorker = true
		}
		if !o.loadedBySnapshotWorker {
			o.offloadedFromSnapshotWorker = true
		}
	}
	if o.offloadedFromNodeHost &&
		o.offloadedFromCommitWorker &&
		o.offloadedFromSnapshotWorker &&
		o.offloadedFromStepWorker {
		o.readyToDestroy = true
	}
}

// IManagedStateMachine is the interface used to manage data store.
type IManagedStateMachine interface {
	Lookup([]byte) ([]byte, error)
	GetHash() uint64
	SaveSnapshot(string, statemachine.ISnapshotFileCollection) (uint64, error)
	RecoverFromSnapshot(string, []statemachine.SnapshotFile) error
	Offloaded(From)
	Loaded(From)
}

// ManagedStateMachineFactory is the factory function type for creating an
// IManagedStateMachine instance.
type ManagedStateMachineFactory func(clusterID uint64,
	nodeID uint64, stopc <-chan struct{}) IManagedStateMachine

// NativeStateMachine is the IManagedStateMachine object used to manage native
// data store in Golang.
type NativeStateMachine struct {
	dataStore statemachine.IStateMachine
	done      <-chan struct{}
	// see dragonboat-doc on how this RWMutex can be entirely avoided
	mu sync.RWMutex
	OffloadedStatus
}

// NewNativeStateMachine creates and returns a new NativeStateMachine object.
// func NewNativeStateMachine(ds statemachine.IStateMachine,
// 	done <-chan struct{}) IManagedStateMachine {
// 	s := &NativeStateMachine{
// 		dataStore: ds,
// 		done:      done,
// 	}
// 	return s
// }
func (ds *NativeStateMachine) closeStateMachine() {
	ds.dataStore.Close()
}

// Offloaded offloads the data store from the specified part of the system.
func (ds *NativeStateMachine) Offloaded(from From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetOffloaded(from)
	if ds.ReadyToDestroy() && !ds.Destroyed() {
		ds.closeStateMachine()
		ds.SetDestroyed()
	}
}

// Loaded marks the statemachine as loaded by the specified component.
func (ds *NativeStateMachine) Loaded(from From) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.SetLoaded(from)
}

// Lookup queries the data store.
func (ds *NativeStateMachine) Lookup(data []byte) ([]byte, error) {
	ds.mu.RLock()
	if ds.Destroyed() {
		ds.mu.RUnlock()
		return nil, ErrGroupClosed
	}
	v := ds.dataStore.Lookup(data)
	ds.mu.RUnlock()
	return v, nil
}

// GetHash returns an integer value representing the state of the data store.
func (ds *NativeStateMachine) GetHash() uint64 {
	return ds.dataStore.GetHash()
}

// SaveSnapshot saves the state of the data store to the snapshot file specified
// by the fp input string.
func (ds *NativeStateMachine) SaveSnapshot(fp string,
	collection statemachine.ISnapshotFileCollection) (uint64, error) {
	writer, err := NewSnapshotWriter(fp)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = writer.Close()
	}()

	sz, err := ds.dataStore.SaveSnapshot(writer, collection, ds.done)
	if err != nil {
		return 0, err
	}
	if err = writer.SaveHeader(sz); err != nil {
		return 0, err
	}
	return sz + SnapshotHeaderSize, nil
}
