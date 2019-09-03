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
	InstanceID uint64
	// SnapshotAvailable bool

	// InitialSnapshot   bool
	// SnapshotRequested bool
	Entries []paxospb.Entry
}

// SMFactoryFunc is the function type for creating an IStateMachine instance
type SMFactoryFunc func(groupID uint64,
	nodeID uint64, done <-chan struct{}) IManagedStateMachine

// INodeProxy is the interface used as proxy to a nodehost.
type INodeProxy interface {
	ApplyUpdate(paxospb.Entry, uint64, bool, bool)
	NodeID() uint64
	GroupID() uint64
}

// StateMachine is a manager class that manages application state
// machine
type StateMachine struct {
	mu          sync.RWMutex
	node        INodeProxy
	sm          IManagedStateMachine
	lastApplied uint64
	members     *paxospb.Membership
	commitC     chan Commit
	aborted     bool
}

//NewStateMachine ...
func NewStateMachine(sm IManagedStateMachine,
	proxy INodeProxy) *StateMachine {
	a := &StateMachine{
		sm:      sm,
		commitC: make(chan Commit, commitChanLength),
		node:    proxy,
	}
	a.members = &paxospb.Membership{
		Addresses: make(map[uint64]string),
		Removed:   make(map[uint64]bool),
	}
	return a
}

// CommitC returns the commit channel.
func (s *StateMachine) CommitC() chan Commit {
	return s.commitC
}

// CommitChanBusy returns whether the CommitC chan is busy. Busy is defined as
// having more than half of its buffer occupied.
func (s *StateMachine) CommitChanBusy() bool {
	return uint64(len(s.commitC)) > commitChanBusyThreshold
}

// GetLastApplied returns the last applied value.
func (s *StateMachine) GetLastApplied() uint64 {
	s.mu.RLock()
	v := s.lastApplied
	s.mu.RUnlock()
	return v
}

// Offloaded marks the state machine as offloaded from the specified component.
func (s *StateMachine) Offloaded(from From) {
	s.sm.Offloaded(from)
}

// Loaded marks the state machine as loaded from the specified component.
func (s *StateMachine) Loaded(from From) {
	s.sm.Loaded(from)
}

// Lookup performances local lookup on the data store.
func (s *StateMachine) Lookup(query []byte) ([]byte, error) {
	s.mu.RLock()
	if s.aborted {
		s.mu.RUnlock()
		return nil, ErrGroupClosed
	}
	result, err := s.sm.Lookup(query)
	s.mu.RUnlock()
	return result, err
}

// GetMembership returns the membership info maintained by the state machine.
func (s *StateMachine) GetMembership() (map[uint64]string,
	map[uint64]struct{}, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	members := make(map[uint64]string)
	removed := make(map[uint64]struct{})
	for nid, addr := range s.members.Addresses {
		members[nid] = addr
	}

	for nid := range s.members.Removed {
		removed[nid] = struct{}{}
	}
	return members, removed, s.members.ConfigChangeId
}

// GetHash returns the state machine hash.
func (s *StateMachine) GetHash() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sm.GetHash()
}

func (s *StateMachine) updateLastApplied(lastApplied uint64) {
	if s.lastApplied+1 != lastApplied {
		plog.Panicf(", node sequential update, last applied %d, applying %d", s.lastApplied, lastApplied)
	}

	if lastApplied == 0 {
		plog.Panicf("lastApplied is 0")
	}
	s.lastApplied = lastApplied
}

func (s *StateMachine) onUpdateApplied(ent paxospb.Entry,
	result uint64, ignored bool, rejected bool) {
	if !ignored {
		s.node.ApplyUpdate(ent, result, rejected, ignored)
	}
}

// Handle pulls the committed record and apply it if there is any available.
func (s *StateMachine) Handle(batch []Commit) (Commit, bool) {
	processed := 0
	batch = batch[:0]
	select {
	case rec := <-s.commitC:
		batch = append(batch, rec)
		processed++
		done := false
		if !done {
			select {
			case rec := <-s.commitC:
				batch = append(batch, rec)
				processed++
			default:
				done = true
			}
		}
	default:
	}
	s.handle(batch)
	return Commit{}, false
}

func (s *StateMachine) handle(batch []Commit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range batch {
		for _, entry := range b.Entries {
			s.updateLastApplied(entry.AcceptorState.InstanceID)
			result := s.sm.Update(entry.AcceptorState.AccetpedValue)
			s.onUpdateApplied(entry, result, false, false)
		}
	}
}

func (s *StateMachine) handleUpdate(ent paxospb.Entry) (uint64, bool, bool) {
	s.mu.Lock()
	s.mu.Unlock()
	s.updateLastApplied(ent.AcceptorState.InstanceID)
	result := s.sm.Update(ent.AcceptorState.AccetpedValue)
	return result, false, false
}

func deepCopyMembership(m paxospb.Membership) paxospb.Membership {
	c := paxospb.Membership{
		ConfigChangeId: m.ConfigChangeId,
		Addresses:      make(map[uint64]string),
		Removed:        make(map[uint64]bool),
	}
	for nid, addr := range m.Addresses {
		c.Addresses[nid] = addr
	}
	for nid := range m.Removed {
		c.Removed[nid] = true
	}
	return c
}
