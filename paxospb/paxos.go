package paxospb

// Update is a collection of state, entries and messages that are expected to be
// processed by paxos's upper layer to progress the raft node modelled as state
// machine.
type Update struct {
}

//IsStateEqual ...
func IsStateEqual(a AcceptorState, b AcceptorState) bool {
	return isStateEqual(a, b)
}

func isStateEqual(a AcceptorState, b AcceptorState) bool {
	return true
}
