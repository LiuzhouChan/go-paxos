package paxos

import (
	"bytes"
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

func getTestAcceptor() *acceptor {
	mi := &mockInstance{
		msgs: make([]paxospb.PaxosMsg, 0),
	}
	return newAcceptor(mi)
}

func TestFirstPrepare(t *testing.T) {
	a := getTestAcceptor()
	msg := paxospb.PaxosMsg{
		MsgType:        paxospb.PaxosPrepare,
		From:           2,
		ProposalID:     1,
		ProposalNodeID: 2,
		Key:            1,
		Value:          []byte("test1"),
	}
	a.handlePrepare(msg)
	mi := a.instance.(*mockInstance)
	if len(mi.msgs) != 1 {
		t.Errorf("the msgs length is %d, want 1", len(mi.msgs))
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg.ProposalID,
		NodeID:     msg.ProposalNodeID,
	}
	if !a.state.PromiseBallot.IsEqual(ballot) {
		t.Errorf("the acceptor propomise ballot is %v, want %v", a.state.PromiseBallot, ballot)
	}
	if len(a.state.AccetpedValue) != 0 {
		t.Errorf("the accepted value is %v, want %v", a.state.AccetpedValue, msg.Value)
	}
	msgReply := mi.msgs[0]
	if msgReply.PreAcceptID != 0 || msgReply.PreAcceptNodeID != 0 {
		t.Errorf("msg reply preaccept should be nil")
	}
}

func TestSecondPrepareWithHigh(t *testing.T) {
	a := getTestAcceptor()
	msg := paxospb.PaxosMsg{
		MsgType:        paxospb.PaxosPrepare,
		From:           2,
		ProposalID:     1,
		ProposalNodeID: 2,
		Key:            1,
		Value:          []byte("test1"),
	}
	a.handlePrepare(msg)
	msg2 := msg
	msg2.ProposalID = 3
	msg2.Value = []byte("test2")
	a.handlePrepare(msg2)
	mi := a.instance.(*mockInstance)
	if len(mi.msgs) != 2 {
		t.Errorf("the msgs length is %d, want 2", len(mi.msgs))
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg2.ProposalID,
		NodeID:     msg2.ProposalNodeID,
	}
	if !a.state.PromiseBallot.IsEqual(ballot) {
		t.Errorf("the acceptor propomise ballot is %v, want %v", a.state.PromiseBallot, ballot)
	}
	if len(a.state.AccetpedValue) != 0 {
		t.Errorf("the accepted value is %v, want %v", a.state.AccetpedValue, msg.Value)
	}
	msgReply := mi.msgs[1]
	if msgReply.PreAcceptID != 0 || msgReply.PreAcceptNodeID != 0 {
		t.Errorf("msg reply preaccept should be nil")
	}
}

func TestSecondPrepareWithLow(t *testing.T) {
	a := getTestAcceptor()
	msg := paxospb.PaxosMsg{
		MsgType:        paxospb.PaxosPrepare,
		From:           2,
		ProposalID:     4,
		ProposalNodeID: 2,
		Key:            1,
		Value:          []byte("test1"),
	}
	a.handlePrepare(msg)
	msg2 := msg
	msg2.ProposalID = 3
	msg2.Value = []byte("test2")
	a.handlePrepare(msg2)

	mi := a.instance.(*mockInstance)
	if len(mi.msgs) != 2 {
		t.Errorf("the msgs length is %d, want 1", len(mi.msgs))
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg.ProposalID,
		NodeID:     msg.ProposalNodeID,
	}
	if !a.state.PromiseBallot.IsEqual(ballot) {
		t.Errorf("the acceptor propomise ballot is %v, want %v", a.state.PromiseBallot, ballot)
	}
	if len(a.state.AccetpedValue) != 0 {
		t.Errorf("the accepted value is %v, want %v", a.state.AccetpedValue, msg.Value)
	}
	msgReply := mi.msgs[1]
	if msgReply.PreAcceptID != 0 || msgReply.PreAcceptNodeID != 0 {
		t.Errorf("msg reply preaccept should be nil")
	}
	if msgReply.RejectByPromiseID != msg.ProposalID {
		t.Errorf("the msgReply reject propomise id is %d, want %d", msgReply.RejectByPromiseID, msg.ProposalID)
	}
}

func TestSecondLowPrepareAfterAccepted(t *testing.T) {
	a := getTestAcceptor()
	msg := paxospb.PaxosMsg{
		MsgType:        paxospb.PaxosPrepare,
		From:           2,
		ProposalID:     4,
		ProposalNodeID: 2,
		Key:            1,
		Value:          []byte("test1"),
	}
	a.handlePrepare(msg)
	msg2 := msg
	msg2.MsgType = paxospb.PaxosAccept
	a.handleAccept(msg2)

	msg3 := msg
	msg3.ProposalID = 3

	a.handlePrepare(msg3)

	mi := a.instance.(*mockInstance)
	if len(mi.msgs) != 3 {
		t.Errorf("the msgs length is %d, want 1", len(mi.msgs))
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg2.ProposalID,
		NodeID:     msg2.ProposalNodeID,
	}
	if !a.state.PromiseBallot.IsEqual(ballot) {
		t.Errorf("the acceptor propomise ballot is %v, want %v", a.state.PromiseBallot, ballot)
	}
	if !a.state.AcceptedBallot.IsEqual(ballot) {
		t.Errorf("the acceptor accepted ballot is %v, want %v", a.state.AcceptedBallot, ballot)
	}
	if !bytes.Equal(a.state.AccetpedValue, msg2.Value) {
		t.Errorf("the acceptor value is %v, want %v", a.state.AccetpedValue, msg2.Value)
	}
	msgReply := mi.msgs[2]
	if msgReply.RejectByPromiseID != ballot.ProposalID {
		t.Errorf("the msgReply reject propomise id is %d, want %d", msgReply.RejectByPromiseID, msg.ProposalID)
	}
}

func TestSecondHighPrepareAfterAccepted(t *testing.T) {
	a := getTestAcceptor()
	msg := paxospb.PaxosMsg{
		MsgType:        paxospb.PaxosPrepare,
		From:           2,
		ProposalID:     4,
		ProposalNodeID: 2,
		Key:            1,
		Value:          []byte("test1"),
	}
	a.handlePrepare(msg)
	msg2 := msg
	msg2.MsgType = paxospb.PaxosAccept
	a.handleAccept(msg2)

	msg3 := msg
	msg3.ProposalID = 10

	a.handlePrepare(msg3)

	mi := a.instance.(*mockInstance)
	if len(mi.msgs) != 3 {
		t.Errorf("the msgs length is %d, want 1", len(mi.msgs))
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg2.ProposalID,
		NodeID:     msg2.ProposalNodeID,
	}
	ballot2 := paxospb.BallotNumber{
		ProposalID: msg3.ProposalID,
		NodeID:     msg3.ProposalNodeID,
	}

	if !a.state.PromiseBallot.IsEqual(ballot2) {
		t.Errorf("the acceptor propomise ballot is %v, want %v", a.state.PromiseBallot, ballot2)
	}

	if !a.state.AcceptedBallot.IsEqual(ballot) {
		t.Errorf("the acceptor accepted ballot is %v, want %v", a.state.AcceptedBallot, ballot)
	}

	if !bytes.Equal(a.state.AccetpedValue, msg2.Value) {
		t.Errorf("the acceptor value is %v, want %v", a.state.AccetpedValue, msg2.Value)
	}

	msgReply := mi.msgs[2]
	ballot3 := paxospb.BallotNumber{
		ProposalID: msgReply.PreAcceptID,
		NodeID:     msgReply.PreAcceptNodeID,
	}
	if !ballot3.IsEqual(ballot) {
		t.Errorf("the preaccept ballot is %v, want %v", ballot3, ballot)
	}
	if !bytes.Equal(msgReply.Value, a.state.AccetpedValue) {
		t.Errorf("the msgReply value is %v, want %v", msgReply.Value, a.state.AccetpedValue)
	}
}

func TestAcceptSame(t *testing.T) {
	a := getTestAcceptor()
	msg := paxospb.PaxosMsg{
		MsgType:        paxospb.PaxosPrepare,
		From:           2,
		ProposalID:     4,
		ProposalNodeID: 2,
		Key:            1,
		Value:          []byte("test1"),
	}
	a.handlePrepare(msg)
	msg2 := msg
	msg2.MsgType = paxospb.PaxosAccept
	a.handleAccept(msg2)
	mi := a.instance.(*mockInstance)
	if len(mi.msgs) != 2 {
		t.Errorf("the msgs length is %d, want 2", len(mi.msgs))
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg.ProposalID,
		NodeID:     msg.ProposalNodeID,
	}
	if !a.state.PromiseBallot.IsEqual(ballot) {
		t.Errorf("the acceptor propomise ballot is %v, want %v", a.state.PromiseBallot, ballot)
	}
	if !a.state.AcceptedBallot.IsEqual(ballot) {
		t.Errorf("the acceptor accepted ballot is %v, want %v", a.state.AcceptedBallot, ballot)
	}
	if !bytes.Equal(a.state.AccetpedValue, msg2.Value) {
		t.Errorf("the acceptor value is %v, want %v", a.state.AccetpedValue, msg2.Value)
	}
}

func TestAcceptLow(t *testing.T) {
	a := getTestAcceptor()
	msg := paxospb.PaxosMsg{
		MsgType:        paxospb.PaxosPrepare,
		From:           2,
		ProposalID:     4,
		ProposalNodeID: 2,
		Key:            1,
		Value:          []byte("test1"),
	}
	a.handlePrepare(msg)
	msg2 := msg
	msg2.MsgType = paxospb.PaxosAccept
	msg2.ProposalID = 3
	a.handleAccept(msg2)
	mi := a.instance.(*mockInstance)
	if len(mi.msgs) != 2 {
		t.Errorf("the msgs length is %d, want 2", len(mi.msgs))
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg.ProposalID,
		NodeID:     msg.ProposalNodeID,
	}
	if !a.state.PromiseBallot.IsEqual(ballot) {
		t.Errorf("the acceptor propomise ballot is %v, want %v", a.state.PromiseBallot, ballot)
	}
	if !a.state.AcceptedBallot.IsNil() {
		t.Errorf("the acceptor accepted ballot is %v, want %v", a.state.AcceptedBallot, ballot)
	}
	if len(a.state.AccetpedValue) != 0 {
		t.Errorf("the acceptor value length is %d, want 0", len(a.state.AccetpedValue))
	}
	if mi.msgs[1].RejectByPromiseID != msg.ProposalID {
		t.Errorf("the msgReply reject propomise id is %d, want %d", mi.msgs[1].RejectByPromiseID, msg.ProposalID)
	}
}

func TestAcceptHigh(t *testing.T) {
	a := getTestAcceptor()
	msg := paxospb.PaxosMsg{
		MsgType:        paxospb.PaxosPrepare,
		From:           2,
		ProposalID:     4,
		ProposalNodeID: 2,
		Key:            1,
		Value:          []byte("test1"),
	}
	a.handlePrepare(msg)
	msg2 := msg
	msg2.MsgType = paxospb.PaxosAccept
	msg2.ProposalID = 10
	a.handleAccept(msg2)
	mi := a.instance.(*mockInstance)
	if len(mi.msgs) != 2 {
		t.Errorf("the msgs length is %d, want 2", len(mi.msgs))
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg2.ProposalID,
		NodeID:     msg2.ProposalNodeID,
	}
	if !a.state.PromiseBallot.IsEqual(ballot) {
		t.Errorf("the acceptor propomise ballot is %v, want %v", a.state.PromiseBallot, ballot)
	}
	if !a.state.AcceptedBallot.IsEqual(ballot) {
		t.Errorf("the acceptor accepted ballot is %v, want %v", a.state.AcceptedBallot, ballot)
	}
	if !bytes.Equal(a.state.AccetpedValue, msg2.Value) {
		t.Errorf("the acceptor value is %v, want %v", a.state.AccetpedValue, msg2.Value)
	}
}

func TestNewInstace(t *testing.T) {
	a := getTestAcceptor()
	msg := paxospb.PaxosMsg{
		MsgType:        paxospb.PaxosPrepare,
		From:           2,
		ProposalID:     4,
		ProposalNodeID: 2,
		Key:            1,
		Value:          []byte("test1"),
	}
	a.handlePrepare(msg)
	msg2 := msg
	msg2.MsgType = paxospb.PaxosAccept
	a.handleAccept(msg2)
	ballot := paxospb.BallotNumber{
		ProposalID: msg.ProposalID,
		NodeID:     msg.ProposalNodeID,
	}
	a.newInstance()
	if a.instanceID != 1 {
		t.Errorf("the instance id is %d, want %d", a.instanceID, 1)
	}
	if !a.state.PromiseBallot.IsEqual(ballot) {
		t.Errorf("the promiseBallot is %v, want %v", a.state.PromiseBallot, ballot)
	}
	if len(a.state.AccetpedValue) != 0 {
		t.Errorf("the acceptor value length is %d, want 0", len(a.state.AccetpedValue))
	}
}
