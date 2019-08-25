package ipaxos

import (
	"github.com/LiuzhouChan/go-paxos/internal/utils/stringutil"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

//Learner ...
type Learner struct {
	instance IInstanceProxy
	nodeID   uint64
	remote   map[uint64]bool

	instanceID uint64

	highestSeenInstanceID           uint64
	highestSeenInstanceIDFromNodeID uint64

	tickCount          uint64
	askForLearnTick    uint64
	askFroLearnTimeout uint64
	isLearned          bool
	learnedValue       []byte
	acceptor           *Acceptor
}

func newLearner(acceptor *Acceptor) *Learner {
	l := &Learner{
		isLearned: false,
		acceptor:  acceptor,
	}
	return l
}

func (l *Learner) learnValueWithoutWrite(value []byte) {
	l.learnedValue = stringutil.BytesDeepCopy(value)
	l.isLearned = true
}

func (l *Learner) newInstance() {
	l.instanceID++
	l.learnedValue = l.learnedValue[:0]
	l.isLearned = false
}

func (l *Learner) isIMLast() bool {
	return l.instanceID+1 >= l.highestSeenInstanceID
}

func (l *Learner) setSeenInstanceID(instanceID, nodeID uint64) {
	if instanceID > l.highestSeenInstanceID {
		l.highestSeenInstanceID = instanceID
		l.highestSeenInstanceIDFromNodeID = nodeID
	}
}

func (l *Learner) tick() {
	l.tickCount++
	l.askForLearnTick++
	if l.timeForAskForLearn() {
		l.askForLearn()
	}
}

func (l *Learner) timeForAskForLearn() bool {
	return l.askForLearnTick >= l.askFroLearnTimeout
}

func (l *Learner) askForLearn() {
	msg := paxospb.PaxosMsg{
		InstanceID: l.instanceID,
		MsgType:    paxospb.PaxosLearnerAskForLearn,
	}
	// broadcast askfor learn msg to peer
	for nid := range l.remote {
		if nid != l.nodeID {
			msg.To = nid
			l.instance.Send(msg)
		}
	}
}

func (l *Learner) handleAskForLearn(msg paxospb.PaxosMsg) {
	l.setSeenInstanceID(msg.InstanceID, msg.NodeID)
	if msg.InstanceID >= l.instanceID {
		return
	}

}

func (l *Learner) sendNowInstanceID(instanceID, nodeID uint64) {
	resp := paxospb.PaxosMsg{
		To:                  nodeID,
		MsgType:             paxospb.PaxosLearnerAskForLearn,
		InstanceID:          instanceID,
		NowInstanceID:       l.instanceID,
		MinChosenInstanceID: 0,
	}
	l.instance.Send(resp)
}

func (l *Learner) handleSendNowInstanceID(msg paxospb.PaxosMsg) {

}

func (l *Learner) comfirmAskForLearn(to uint64) {
	msg := paxospb.PaxosMsg{
		To:         to,
		InstanceID: l.instanceID,
		MsgType:    paxospb.PaxosLearnerConfirmAskForLearn,
	}
	l.instance.Send(msg)
	l.isLearned = true
}

func (l *Learner) handleComfirmAskForLearn(msg paxospb.PaxosMsg) {

}

func (l *Learner) sendLearnValue(to uint64, learnInstanceID uint64,
	learnedBallot paxospb.BallotNumber, learnedValue []byte) {
	msg := paxospb.PaxosMsg{
		To:         to,
		MsgType:    paxospb.PaxosLearnerSendLearnValue,
		InstanceID: learnInstanceID,
		ProposalID: learnedBallot.ProposalID,
		Value:      stringutil.BytesDeepCopy(learnedValue),
	}
	l.instance.Send(msg)
}

func (l *Learner) handleSendLearnValue(msg paxospb.PaxosMsg) {
	plog.Infof("get learn instance id %v, while our instance id %v", msg.InstanceID, l.instanceID)
	if msg.InstanceID > l.instanceID {
		plog.Infof("cannot learn")
		return
	}
	if msg.InstanceID < l.instanceID {
		plog.Infof("no need to learn")
	} else {
		// learn value
		// ballot := paxospb.BallotNumber{
		// 	ProposalID: msg.ProposalID,
		// 	NodeID:     msg.ProposalNodeID,
		// }
		l.learnValueWithoutWrite(msg.Value)
	}

}

func (l *Learner) proposerSendSuccess(learnInstanceID, proposalID uint64) {
	msg := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosLearnerProposerSendSuccess,
		InstanceID: learnInstanceID,
		ProposalID: proposalID,
	}
	l.instance.Send(msg)
}

func (l *Learner) handleProposerSendSuccess(msg paxospb.PaxosMsg) {
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
