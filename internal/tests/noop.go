package tests

// NoOP is a IStateMachine struct used for testing purpose.
type NoOP struct {
}

// Lookup locally looks up the data.
func (n *NoOP) Lookup(key []byte) []byte {
	return make([]byte, 1)
}

// Update updates the object.
func (n *NoOP) Update(data []byte) uint64 {
	return uint64(len(data))
}

// Close closes the NoOP IStateMachine.
func (n *NoOP) Close() {}

// GetHash returns a uint64 value representing the current state of the object.
func (n *NoOP) GetHash() uint64 {
	// the hash value is always 0, so it is of course always consistent
	return 0
}
