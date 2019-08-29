package paxos

type remote struct {
	match uint64
	next  uint64
}
