package paxos

import (
	"bytes"
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

func getTestLearner() *learner {
	acceptor := getTestAcceptor()
	l := newLearner(acceptor.instance, acceptor)
	l.askForLearnTimeout = 10
	l.instanceID = l.instance.getLog().committed
	return l
}

func TestInit(t *testing.T) {
	l := getTestLearner()
	if len(l.learnedValue) != 0 {
		t.Errorf("len(l.learnedValue) is %d, want 0", len(l.learnedValue))
	}
	if l.isLearned {
		t.Errorf("l.isLearned should be false")
	}
	if l.highestSeenInstanceID != 0 {
		t.Errorf("l.highestSeenInstanceID is %d, should be 0", l.highestSeenInstanceID)
	}
	if l.highestSeenInstanceIDFromNodeID != 0 {
		t.Errorf("t.highestSeenInstanceIDFromNodeID is %d, should be 0", l.highestSeenInstanceIDFromNodeID)
	}
}

func TestAskForLearn(t *testing.T) {
	l := getTestLearner()
	for i := uint64(0); i < l.askForLearnTimeout; i++ {
		l.tick()
	}
	if l.askForLearnTick != 0 {
		t.Errorf("l.askForLearnTick is %d, want 0", l.askForLearnTick)
	}
	msgs := l.instance.readMsgs()
	remotes := l.instance.getRemotes()
	if len(msgs) != len(remotes)-1 {
		t.Errorf("len(msgs) is %d, want %d", len(msgs), len(remotes)-1)
	}
	for _, m := range msgs {
		if m.From == l.instance.getNodeID() {
			t.Errorf("send askfor learn to self %d", l.instance.getNodeID())
		}
	}
}

func TestHandleAskForLearnHighInstanceID(t *testing.T) {
	l := getTestLearner()
	msg := paxospb.PaxosMsg{
		InstanceID: 10,
		From:       2,
	}
	l.handleAskForLearn(msg)
	msgs := l.instance.readMsgs()
	if len(msgs) != 0 {
		t.Errorf("len(msgs) is %d, want 0", len(msgs))
	}
	if l.highestSeenInstanceID != msg.InstanceID {
		t.Errorf("l.highestSeenInstanceID is %d, want %d", l.highestSeenInstanceID, msg.InstanceID)
	}
	if l.highestSeenInstanceIDFromNodeID != msg.From {
		t.Errorf("l.highestSeenInstanceIDFromNodeID is %d, want %d", l.highestSeenInstanceIDFromNodeID, msg.From)
	}
}

func TestHandleAskForLearnLowInstanceID(t *testing.T) {
	l := getTestLearner()
	msg := paxospb.PaxosMsg{
		InstanceID: 1,
		From:       2,
	}
	l.handleAskForLearn(msg)
	msgs := l.instance.readMsgs()
	if uint64(len(msgs)) != l.instanceID-msg.InstanceID+1 {
		t.Errorf("len(msgs) is %d, want %d", len(msgs), l.instanceID-msg.InstanceID+1)
	}
}

func TestHandleLearnValue(t *testing.T) {
	l := getTestLearner()
	l.instanceID++
	value := []byte("testValue")
	msg := paxospb.PaxosMsg{
		To:             1,
		MsgType:        paxospb.PaxosLearnerSendLearnValue,
		InstanceID:     11,
		ProposalID:     2,
		ProposalNodeID: 2,
		Key:            32,
		Value:          value,
	}
	l.handleSendLearnValue(msg)
	if !l.isLearned {
		t.Errorf("l.isLearned should be true")
	}
	if !bytes.Equal(l.learnedValue, value) {
		t.Errorf("l.learnedValue is %v, want %v", l.learnedValue, value)
	}
	if l.key != uint64(32) {
		t.Errorf("l.key is %d, want 32", l.key)
	}
}

func TestLearnerNewInstance(t *testing.T) {
	l := getTestLearner()
	l.instanceID++
	value := []byte("testValue")
	msg := paxospb.PaxosMsg{
		To:             1,
		MsgType:        paxospb.PaxosLearnerSendLearnValue,
		InstanceID:     11,
		ProposalID:     2,
		ProposalNodeID: 2,
		Key:            32,
		Value:          value,
	}
	l.handleSendLearnValue(msg)
	l.newInstance()
	if l.key != 0 {
		t.Errorf("l.key is %d, want 0", l.key)
	}
	if len(l.learnedValue) != 0 {
		t.Errorf("len(l.learnedValue) is %d, want 0", len(l.learnedValue))
	}
	if l.isLearned {
		t.Errorf("l.isLearned should be false")
	}
	if l.instanceID != 12 {
		t.Errorf("l.instanceID is %d, want 12", l.instanceID)
	}
}
