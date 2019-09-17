package main

import (
	"encoding/binary"
	"fmt"

	sm "github.com/LiuzhouChan/go-paxos/statemachine"
)

// ExampleStateMachine is the IStateMachine implementation used in the
// helloworld example.
type ExampleStateMachine struct {
	GroupID uint64
	NodeID  uint64
	Count   uint64
}

// NewExampleStateMachine creates and return a new ExampleStateMachine object.
func NewExampleStateMachine(groupID uint64, nodeID uint64) sm.IStateMachine {
	return &ExampleStateMachine{
		GroupID: groupID,
		NodeID:  nodeID,
		Count:   0,
	}
}

// Lookup performs local lookup on the ExampleStateMachine instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *ExampleStateMachine) Lookup(query []byte) []byte {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, s.Count)
	return result
}

// Update updates the object using the specified committed paxos entry.
func (s *ExampleStateMachine) Update(data []byte) uint64 {
	s.Count++
	fmt.Printf("from ExampleStateMachine.Update(), msg: %s, count:%d\n",
		string(data), s.Count)
	return uint64(len(data))
}

// Close closes the IStateMachine instance. There is nothing for us to cleanup
// or release as this is a pure in memory data store. Note that the Close
// method is not guaranteed to be called as node can crash at any time.
func (s *ExampleStateMachine) Close() {}

// GetHash returns a uint64 representing the current object state.
func (s *ExampleStateMachine) GetHash() uint64 {
	// the only state we have is that Count variable. that uint64 value pretty much
	// represents the state of this IStateMachine
	return s.Count
}
