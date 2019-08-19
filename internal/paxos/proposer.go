package ipaxos

import (
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

//Proposer ...
type Proposer struct {
	instance                    IInstanceProxy
	instanceID                  uint64
	proposalID                  uint64
	highestOtherProposalID      uint64
	value                       string
	highestOtherPreAcceptBallot paxospb.BallotNumber
	canSkipPrepare              bool
}

func newProposer() *Proposer {
	p := &Proposer{
		proposalID: 1,
		value:      "",
	}
	return p
}

func (p *Proposer) reset() {
	p.highestOtherProposalID = 0
	p.value = ""
}

func (p *Proposer) newPrepare() {
	maxProposalID := p.highestOtherProposalID
	if p.proposalID >= maxProposalID {
		maxProposalID = p.proposalID
	}
	p.proposalID = maxProposalID + 1
}
