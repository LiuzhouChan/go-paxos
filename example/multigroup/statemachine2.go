package main

import (
	"encoding/binary"
	"fmt"

	"github.com/LiuzhouChan/go-paxos/statemachine"
)

// SecondStateMachine is the IStateMachine implementation used in the
// multigroup example for handling all inputs ends with "?".
type SecondStateMachine struct {
	GroupID uint64
	NodeID  uint64
	Count   uint64
}

// NewSecondStateMachine creates and return a new SecondStateMachine object.
func NewSecondStateMachine(clusterID uint64,
	nodeID uint64) statemachine.IStateMachine {
	return &SecondStateMachine{
		GroupID: clusterID,
		NodeID:  nodeID,
		Count:   0,
	}
}

// Lookup performs local lookup on the SecondStateMachine instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *SecondStateMachine) Lookup(query []byte) []byte {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, s.Count)
	return result
}

// Update updates the object using the specified committed paxos entry.
func (s *SecondStateMachine) Update(data []byte) uint64 {
	// in this example, we regard the input as a question.
	s.Count++
	fmt.Printf("got a question from user: %s, count:%d\n", string(data), s.Count)
	return uint64(len(data))
}

// Close closes the IStateMachine instance. There is nothing for us to cleanup
// or release as this is a pure in memory data store. Note that the Close
// method is not guaranteed to be called as node can crash at any time.
func (s *SecondStateMachine) Close() {}

// GetHash returns a uint64 representing the current object state.
func (s *SecondStateMachine) GetHash() uint64 {
	// the only state we have is that Count variable. that uint64 value pretty much
	// represents the state of this IStateMachine
	return s.Count
}
