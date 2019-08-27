package paxospb

var (
	emptyState = State{}
)

// UpdateCommit is used to describe how to commit the Update instance to
// progress the state of paxos
type UpdateCommit struct {
	AppliedTo uint64
}

// Update is a collection of state, entries and messages that are expected to be
// processed by paxos's upper layer to progress the raft node modelled as state
// machine.
type Update struct {
	State
	GroupID          uint64
	NodeID           uint64
	EntriesToSave    []Entry
	CommittedEntries []Entry
	Messages         []PaxosMsg
	LastApplied      uint64
	UpdateCommit     UpdateCommit
}

//IsAcceptorStateEqual ...
func IsAcceptorStateEqual(a AcceptorState, b AcceptorState) bool {
	return isAcceptorStateEqual(a, b)
}

func isAcceptorStateEqual(a AcceptorState, b AcceptorState) bool {
	return true
}

//IsStateEqual ...
func IsStateEqual(a State, b State) bool {
	return isStateEqual(a, b)
}

//IsEmptyState ...
func IsEmptyState(a State) bool {
	return isStateEqual(a, emptyState)
}

func isStateEqual(a State, b State) bool {
	return a.Commit == b.Commit
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
