package statemachine

// IStateMachine is the interface required to be implemented by application's
// state machine, it defines how application data should be internally stored,
// updated and queried.
type IStateMachine interface {
	Update([]byte) uint64
	Lookup([]byte) []byte
	Close()
	// OPtional
	GetHash() uint64
}
