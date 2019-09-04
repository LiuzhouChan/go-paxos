package paxos

import (
	"github.com/LiuzhouChan/go-paxos/internal/utils/stringutil"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

//learner ...
type learner struct {
	instance *instance
	acceptor *acceptor
	nodeID   uint64
	remote   map[uint64]bool

	instanceID uint64

	highestSeenInstanceID           uint64
	highestSeenInstanceIDFromNodeID uint64

	askForLearnTick    uint64
	askFroLearnTimeout uint64

	isIMlearning bool
	isLearned    bool
	learnedValue []byte
}

func newLearner(i *instance, acceptor *acceptor) *learner {
	l := &learner{
		instance:     i,
		isLearned:    false,
		acceptor:     acceptor,
		learnedValue: []byte{},
	}
	return l
}

func (l *learner) learnValueWithoutWrite(value []byte) {
	l.learnedValue = stringutil.BytesDeepCopy(value)
	l.isLearned = true
}

func (l *learner) newInstance() {
	l.instanceID++
	l.learnedValue = l.learnedValue[:0]
	l.isLearned = false
}

func (l *learner) isIMLast() bool {
	return l.instanceID+1 >= l.highestSeenInstanceID
}

func (l *learner) setSeenInstanceID(instanceID, nodeID uint64) {
	if instanceID > l.highestSeenInstanceID {
		l.highestSeenInstanceID = instanceID
		l.highestSeenInstanceIDFromNodeID = nodeID
	}
}

func (l *learner) tick() {
	l.askForLearnTick++
	if l.timeForAskForLearn() {
		l.askForLearn()
	}
}

func (l *learner) timeForAskForLearn() bool {
	return l.askForLearnTick >= l.askFroLearnTimeout
}

func (l *learner) askForLearn() {
	plog.Infof("start ask for learn")
	msg := paxospb.PaxosMsg{
		InstanceID: l.instanceID,
		MsgType:    paxospb.PaxosLearnerAskForLearn,
	}
	// broadcast askfor learn msg to peer
	for nid := range l.remote {
		if nid != l.nodeID {
			msg.To = nid
			l.instance.send(msg)
		}
	}
}

func (l *learner) handleAskForLearn(msg paxospb.PaxosMsg) {
	l.setSeenInstanceID(msg.InstanceID, msg.From)
	if msg.InstanceID >= l.instanceID {
		return
	}
	l.sendNowInstanceID(msg.InstanceID, msg.From)
}

func (l *learner) sendNowInstanceID(instanceID, to uint64) {
	resp := paxospb.PaxosMsg{
		To:            to,
		MsgType:       paxospb.PaxosLearnerAskForLearn,
		InstanceID:    instanceID,
		NowInstanceID: l.instanceID,
	}
	l.instance.send(resp)
}

func (l *learner) handleSendNowInstanceID(msg paxospb.PaxosMsg) {
	// we get the instance id of others right now
	l.setSeenInstanceID(msg.NowInstanceID, msg.From)
	if msg.InstanceID != l.instanceID {
		plog.Infof("lag msg, skip")
		return
	}
	if msg.NowInstanceID <= l.instanceID {
		plog.Infof("lag msg, skip")
		return
	}
	if !l.isIMlearning {
		l.comfirmAskForLearn(msg.From)
	}
}

func (l *learner) comfirmAskForLearn(to uint64) {
	msg := paxospb.PaxosMsg{
		To:         to,
		InstanceID: l.instanceID,
		MsgType:    paxospb.PaxosLearnerConfirmAskForLearn,
	}
	l.instance.send(msg)
	l.isIMlearning = true
}

func (l *learner) handleComfirmAskForLearn(msg paxospb.PaxosMsg) {
	// send replicate msg now
}

func (l *learner) sendLearnValue(to uint64, learnInstanceID uint64,
	learnedBallot paxospb.BallotNumber, learnedValue []byte) {
	msg := paxospb.PaxosMsg{
		To:             to,
		MsgType:        paxospb.PaxosLearnerSendLearnValue,
		InstanceID:     learnInstanceID,
		ProposalID:     learnedBallot.ProposalID,
		ProposalNodeID: learnedBallot.NodeID,
		Value:          stringutil.BytesDeepCopy(learnedValue),
	}
	l.instance.send(msg)
}

func (l *learner) handleSendLearnValue(msg paxospb.PaxosMsg) {
	plog.Infof("get learn instance id %v, while our instance id %v", msg.InstanceID, l.instanceID)
	if msg.InstanceID > l.instanceID {
		plog.Infof("cannot learn")
		return
	}
	if msg.InstanceID < l.instanceID {
		plog.Infof("no need to learn")
	} else {
		l.learnValueWithoutWrite(msg.Value)
	}

}

func (l *learner) proposerSendSuccess(learnInstanceID, proposalID uint64) {
	msg := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosLearnerProposerSendSuccess,
		InstanceID: learnInstanceID,
		ProposalID: proposalID,
	}
	for nid := range l.remote {
		msg.To = nid
		l.instance.send(msg)
	}
}

func (l *learner) handleProposerSendSuccess(msg paxospb.PaxosMsg) {
	plog.Infof("get proposal send success msg instance id %v, while ours %v", msg.InstanceID, l.instanceID)
	if msg.InstanceID != l.instanceID {
		plog.Infof("Instance id not same, skip")
		return
	}
	if l.acceptor.state.AcceptedBallot.IsNil() {
		plog.Infof("haven't accepted any proposal")
		return
	}
	ballot := paxospb.BallotNumber{
		ProposalID: msg.ProposalID,
		NodeID:     msg.From,
	}
	if !l.acceptor.state.AcceptedBallot.IsEqual(ballot) {
		plog.Infof("proposal ballot not same to accepted ballot")
		return
	}
	l.learnValueWithoutWrite(msg.Value)
	plog.Infof("learn value ok")
}
