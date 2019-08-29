package paxos

import "github.com/LiuzhouChan/go-paxos/paxospb"

//acceptor ...
type acceptor struct {
	instance   IInstanceProxy
	instanceID uint64
	nodeID     uint64
	state      paxospb.AcceptorState
}

func (a *acceptor) newInstance() {
	a.instanceID++
	a.state = paxospb.AcceptorState{}
}

func (a *acceptor) handlePrepare(msg paxospb.PaxosMsg) {
	resp := paxospb.PaxosMsg{
		To:         msg.From,
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
		a.state.PromiseBallot = ballot
	} else {
		// if our's proposal id is bigger
		resp.RejectByPromiseID = a.state.PromiseBallot.ProposalID
	}
	a.instance.Send(resp)
}

func (a *acceptor) handleAccept(msg paxospb.PaxosMsg) {
	resp := paxospb.PaxosMsg{
		To:         msg.From,
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
		a.state.AccetpedValue = msg.Value
	} else {
		resp.RejectByPromiseID = a.state.PromiseBallot.ProposalID
	}

	a.instance.Send(resp)
}

//GetAcceptorState ...
func (a *acceptor) GetAcceptorState() paxospb.AcceptorState {
	return a.state
}
