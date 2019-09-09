package paxos

import (
	"github.com/LiuzhouChan/go-paxos/internal/utils/stringutil"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

//acceptor ...
type acceptor struct {
	instance   IInstance
	instanceID uint64
	acceptKey  uint64
	state      paxospb.AcceptorState
}

func newAcceptor(i IInstance) *acceptor {
	return &acceptor{
		instance: i,
	}
}

func (a *acceptor) newInstance() {
	a.instanceID++
	a.acceptKey = 0
	a.state.AccetpedValue = a.state.AccetpedValue[:0]
	a.state.AcceptedBallot = paxospb.BallotNumber{}
}

func (a *acceptor) handlePrepare(msg paxospb.PaxosMsg) {
	resp := paxospb.PaxosMsg{
		To:         msg.From,
		InstanceID: a.instanceID,
		MsgType:    paxospb.PaxosPrepareReply,
		ProposalID: msg.ProposalID,
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg.ProposalID,
		NodeID:     msg.From,
	}

	if ballot.IsNotLessThan(a.state.PromiseBallot) {
		// if the msg's proposal id is bigger than ours
		resp.PreAcceptID = a.state.AcceptedBallot.ProposalID
		resp.PreAcceptNodeID = a.state.AcceptedBallot.NodeID
		if a.state.AcceptedBallot.ProposalID > 0 {
			resp.Value = stringutil.BytesDeepCopy(a.state.AccetpedValue)
			resp.Key = a.acceptKey
		}
		a.state.PromiseBallot = ballot
	} else {
		// if our's proposal id is bigger
		resp.RejectByPromiseID = a.state.PromiseBallot.ProposalID
	}
	a.instance.send(resp)
}

func (a *acceptor) handleAccept(msg paxospb.PaxosMsg) {
	resp := paxospb.PaxosMsg{
		To:         msg.From,
		InstanceID: a.instanceID,
		MsgType:    paxospb.PaxosAcceptReply,
		ProposalID: msg.ProposalID,
	}

	ballot := paxospb.BallotNumber{
		ProposalID: msg.ProposalID,
		NodeID:     msg.From,
	}

	if ballot.IsNotLessThan(a.state.PromiseBallot) {
		a.state.PromiseBallot = ballot
		a.state.AcceptedBallot = ballot
		a.state.AccetpedValue = stringutil.BytesDeepCopy(msg.Value)
		a.acceptKey = msg.Key
	} else {
		resp.RejectByPromiseID = a.state.PromiseBallot.ProposalID
	}

	a.instance.send(resp)
}

//GetAcceptorState ...
func (a *acceptor) GetAcceptorState() paxospb.AcceptorState {
	return a.state
}
