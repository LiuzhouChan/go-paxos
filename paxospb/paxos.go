package paxospb

// Update is a collection of state, entries and messages that are expected to be
// processed by paxos's upper layer to progress the raft node modelled as state
// machine.
type Update struct {
	AcceptorState
	GroupID          uint64
	NodeID           uint64
	EntriesToSave    []Entry
	CommittedEntries []Entry
	Messages         []PaxosMsg
	LastApplied      uint64
}

//IsStateEqual ...
func IsStateEqual(a AcceptorState, b AcceptorState) bool {
	return isStateEqual(a, b)
}

func isStateEqual(a AcceptorState, b AcceptorState) bool {
	return true
}

//IsNil ...
func (m *BallotNumber) IsNil() bool {
	return m.ProposalID == 0
}

//IsEqual ...
func (m *BallotNumber) IsEqual(b BallotNumber) bool {
	return m.ProposalID == b.ProposalID && m.NodeID == b.NodeID
}

//IsNotLessThan ...
func (m *BallotNumber) IsNotLessThan(b BallotNumber) bool {
	if m.ProposalID == b.ProposalID {
		return m.NodeID >= b.NodeID
	}
	return m.ProposalID >= b.ProposalID
}
