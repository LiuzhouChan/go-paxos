package paxos

import (
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

func getTestProposer() *proposer {
	l := getTestLearner()
	proposer := newProposer(l.instance, l)
	proposer.prepareTimeout = 10
	proposer.acceptTimeout = 5
	return proposer
}

func TestPrepare(t *testing.T) {
	p := getTestProposer()
	p.prepare(false)

	if p.st != preparing {
		t.Errorf("the state should be preparing")
	}
	if p.proposalID != 1 {
		t.Errorf("the proposal id should be 1")
	}
	if p.canSkipPrepare {
		t.Errorf("cannot skip prepare")
	}
	if p.rejectBySomeone {
		t.Errorf("should not reject by someone")
	}
	msgs := p.instance.readMsgs()
	remotes := p.instance.getRemotes()
	if len(msgs) != len(remotes) {
		t.Errorf("the msgs len is %d, want %d", len(msgs), len(remotes))
	}
	for _, tt := range msgs {
		if tt.InstanceID != p.instanceID || tt.ProposalID != p.proposalID || tt.MsgType != paxospb.PaxosPrepare {
			t.Errorf("the broadcast msg err")
		}
	}
}

func TestPrepareTimeout(t *testing.T) {
	p := getTestProposer()
	p.prepare(false)
	msgs := p.instance.readMsgs()
	for i := uint64(0); i < p.prepareTimeout; i++ {
		p.tick()
	}
	if p.st != preparing {
		t.Errorf("the state should be preparing")
	}
	if p.preparingTick != 0 {
		t.Errorf("the preparing tick is %d, want 0", p.preparingTick)
	}
	if len(msgs) != len(p.instance.getRemotes()) {
		t.Errorf("the mi msgs len is %d, want %d", len(msgs), len(p.instance.getRemotes()))
	}
}

func TestHandlePrepareReply(t *testing.T) {
	p := getTestProposer()
	p.prepare(false)
	p.instance.readMsgs()
	if len(p.votes) != 0 {
		t.Errorf("the len of p.vote should be 0")
	}
	msg1 := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosPrepareReply,
		From:       1,
		ProposalID: p.proposalID,
	}
	msg2 := msg1
	msg2.From = 2
	msg3 := msg1
	msg3.From = 3
	p.handlePrepareReply(msg1)
	if !p.votes[msg1.From] {
		t.Errorf("the vote from %d should be true", msg1.From)
	}
	p.handlePrepareReply(msg2)
	p.handlePrepareReply(msg3)
	if !p.canSkipPrepare {
		t.Errorf("can skip prepare should be true")
	}
	if len(p.votes) != 0 {
		t.Errorf("the len of p.vote should be 0")
	}
	msgs := p.instance.readMsgs()
	if len(msgs) != len(p.instance.getRemotes()) {
		t.Errorf("the send msgs len is %d, want %d", len(msgs), len(p.instance.getRemotes()))
	}
	if p.st != accepting {
		t.Errorf("the state should be accepting after prepare")
	}
}

func TestHandleAcceptReply(t *testing.T) {
	p := getTestProposer()
	p.prepare(false)
	p.instance.readMsgs()
	if len(p.votes) != 0 {
		t.Errorf("the len of p.vote should be 0")
	}
	msg1 := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosPrepareReply,
		From:       1,
		ProposalID: p.proposalID,
	}
	msg2 := msg1
	msg2.From = 2
	msg3 := msg1
	msg3.From = 3
	p.handlePrepareReply(msg1)
	p.handlePrepareReply(msg2)
	p.handlePrepareReply(msg3)
	p.instance.readMsgs()

	if !p.canSkipPrepare {
		t.Errorf("canSkipPrepare should be true")
	}

	msg4 := msg1
	msg4.MsgType = paxospb.PaxosAcceptReply
	msg4.From = 1
	p.handleAcceptReply(msg4)
	msg5 := msg4
	msg5.From = 2
	msg6 := msg5
	msg6.From = 3
	p.handleAcceptReply(msg5)
	p.handleAcceptReply(msg6)
	msgs := p.instance.readMsgs()
	if len(msgs) != len(p.instance.getRemotes()) {
		t.Errorf("the len of msgs is %d, want %d", len(msgs), len(p.instance.getRemotes()))
	}
	if msgs[0].MsgType != paxospb.PaxosLearnerProposerSendSuccess {
		t.Errorf("msgs[0].MsgType is %d, want %d", msgs[0].MsgType, paxospb.PaxosLearnerProposerSendSuccess)
	}
	if p.st != closing {
		t.Errorf("the state after acceptor should be cloing")
	}

}

func TestAcceptTimeout(t *testing.T) {
	p := getTestProposer()
	p.prepare(false)
	p.instance.readMsgs()
	if len(p.votes) != 0 {
		t.Errorf("the len of p.vote should be 0")
	}
	msg1 := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosPrepareReply,
		From:       1,
		ProposalID: p.proposalID,
	}
	msg2 := msg1
	msg2.From = 2
	msg3 := msg1
	msg3.From = 3
	p.handlePrepareReply(msg1)
	p.handlePrepareReply(msg2)
	p.handlePrepareReply(msg3)
	p.instance.readMsgs()
	if p.acceptingTick != 0 {
		t.Errorf("the acceptingTick is %d, want 0", p.acceptingTick)
	}
	for i := uint64(0); i < p.acceptTimeout; i++ {
		p.tick()
	}
	if p.acceptingTick != 0 {
		t.Errorf("the acceptingTick is %d, want 0", p.acceptingTick)
	}
	if p.st != preparing {
		t.Errorf("the state of proposor should be preparing")
	}
	msgs := p.instance.readMsgs()
	if len(msgs) != len(p.instance.getRemotes()) {
		t.Errorf("the len(msgs) is %d, want %d", len(msgs), len(p.instance.getRemotes()))
	}
}

func TestAcceptTimeoutWithReject(t *testing.T) {
	p := getTestProposer()
	p.prepare(false)
	p.instance.readMsgs()
	if len(p.votes) != 0 {
		t.Errorf("the len of p.vote should be 0")
	}
	msg1 := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosPrepareReply,
		From:       1,
		ProposalID: p.proposalID,
	}
	msg2 := msg1
	msg2.From = 2
	msg3 := msg1
	msg3.From = 3
	p.handlePrepareReply(msg1)
	p.handlePrepareReply(msg2)
	p.handlePrepareReply(msg3)
	p.instance.readMsgs()

	if !p.canSkipPrepare {
		t.Errorf("canSkipPrepare should be true")
	}

	msg4 := msg1
	msg4.MsgType = paxospb.PaxosAcceptReply
	msg4.From = 1
	msg4.RejectByPromiseID = 100
	p.handleAcceptReply(msg4)
	p.instance.readMsgs()
	for i := uint64(0); i < p.acceptTimeout; i++ {
		p.tick()
	}
	if p.acceptingTick != 0 {
		t.Errorf("the acceptingTick is %d, want 0", p.acceptingTick)
	}
	if p.st != preparing {
		t.Errorf("the state of proposor should be preparing")
	}
	msgs := p.instance.readMsgs()
	if len(msgs) != len(p.instance.getRemotes()) {
		t.Errorf("the len(msgs) is %d, want %d", len(msgs), len(p.instance.getRemotes()))
	}
	if p.proposalID != 101 {
		t.Errorf("the proposal id is %d, want 101", p.proposalID)
	}
	if p.canSkipPrepare {
		t.Errorf("canSkipPrepare should be false")
	}
}

func TestProposerNewInstance(t *testing.T) {
	p := getTestProposer()
	p.prepare(false)
	p.instance.readMsgs()
	if len(p.votes) != 0 {
		t.Errorf("the len of p.vote should be 0")
	}
	msg1 := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosPrepareReply,
		From:       1,
		ProposalID: p.proposalID,
	}
	msg2 := msg1
	msg2.From = 2
	msg3 := msg1
	msg3.From = 3
	p.handlePrepareReply(msg1)
	p.handlePrepareReply(msg2)
	p.handlePrepareReply(msg3)
	p.instance.readMsgs()

	p.newInstance()
	if !p.canSkipPrepare {
		t.Errorf("canSkipPrepare should be true")
	}
	if p.acceptingTick != 0 || p.preparingTick != 0 {
		t.Errorf("the tick should be 0")
	}
	if len(p.value) != 0 {
		t.Errorf("len(p.value) is %d, want 0", len(p.value))
	}
	if len(p.votes) != 0 {
		t.Errorf("len(p.votes) is %d, want 0", len(p.value))
	}
	if p.highestOtherProposalID != 0 {
		t.Errorf("p.highestOtherProposalID ")
	}
}
