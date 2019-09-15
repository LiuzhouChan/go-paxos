package paxos

import (
	"github.com/LiuzhouChan/go-paxos/internal/utils/stringutil"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

//learner ...
type learner struct {
	instance IInstance
	acceptor *acceptor

	instanceID uint64

	highestSeenInstanceID           uint64
	highestSeenInstanceIDFromNodeID uint64

	askForLearnTick    uint64
	askFroLearnTimeout uint64

	isLearned    bool
	key          uint64
	learnedValue []byte
}

func newLearner(i IInstance, acceptor *acceptor) *learner {
	l := &learner{
		instance:     i,
		isLearned:    false,
		acceptor:     acceptor,
		learnedValue: []byte{},
	}
	return l
}

func (l *learner) learnValueWithoutWrite(key uint64, value []byte) {
	l.key = key
	l.learnedValue = stringutil.BytesDeepCopy(value)
	l.isLearned = true
}

func (l *learner) newInstance() {
	l.instanceID++
	l.key = 0
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
		l.askForLearnTick = 0
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
	remotes := l.instance.getRemotes()
	// broadcast askfor learn msg to peer
	plog.Infof("the remotes is: %v", l.instance.getRemotes())
	for nid := range remotes {
		if nid != l.instance.getNodeID() {
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
	// l.sendNowInstanceID(msg.InstanceID, msg.From)
	if msg.InstanceID < l.instance.getLog().firstInstanceID() {
		plog.Panicf("the instance id is less than the first instance id: %d, %d",
			msg.InstanceID, l.instance.getLog().firstInstanceID())
	}
	ents, err := l.instance.getLog().getEntries(msg.InstanceID, l.instance.getLog().committed+1)
	if err != nil {
		plog.Errorf("%v", err)
		return
	}
	for _, ent := range ents {
		l.sendLearnValue(msg.From, ent.AcceptorState.InstanceID,
			ent.AcceptorState.AcceptedBallot, ent.Key, ent.AcceptorState.AccetpedValue)
	}
}

// func (l *learner) sendNowInstanceID(instanceID, to uint64) {
// 	resp := paxospb.PaxosMsg{
// 		To:            to,
// 		MsgType:       paxospb.PaxosLearnerAskForLearn,
// 		InstanceID:    instanceID,
// 		NowInstanceID: l.instanceID,
// 	}
// 	l.instance.send(resp)
// }

// func (l *learner) handleSendNowInstanceID(msg paxospb.PaxosMsg) {
// 	// we get the instance id of others right now
// 	l.setSeenInstanceID(msg.NowInstanceID, msg.From)
// 	if msg.InstanceID != l.instanceID {
// 		plog.Infof("lag msg, skip")
// 		return
// 	}
// 	if msg.NowInstanceID <= l.instanceID {
// 		plog.Infof("lag msg, skip")
// 		return
// 	}
// 	if !l.isIMlearning {
// 		l.comfirmAskForLearn(msg.From)
// 	}
// }

// func (l *learner) comfirmAskForLearn(to uint64) {
// 	msg := paxospb.PaxosMsg{
// 		To:         to,
// 		InstanceID: l.instanceID,
// 		MsgType:    paxospb.PaxosLearnerConfirmAskForLearn,
// 	}
// 	l.instance.send(msg)
// 	l.isIMlearning = true
// }

// func (l *learner) handleComfirmAskForLearn(msg paxospb.PaxosMsg) {
// 	// send replicate msg now
// }

func (l *learner) sendLearnValue(to uint64, learnInstanceID uint64,
	learnedBallot paxospb.BallotNumber, key uint64, learnedValue []byte) {
	msg := paxospb.PaxosMsg{
		To:             to,
		MsgType:        paxospb.PaxosLearnerSendLearnValue,
		InstanceID:     learnInstanceID,
		ProposalID:     learnedBallot.ProposalID,
		ProposalNodeID: learnedBallot.NodeID,
		Key:            key,
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
		l.learnValueWithoutWrite(msg.Key, msg.Value)
	}
}

func (l *learner) proposerSendSuccess(learnInstanceID, proposalID uint64) {
	msg := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosLearnerProposerSendSuccess,
		InstanceID: learnInstanceID,
		ProposalID: proposalID,
	}
	remotes := l.instance.getRemotes()
	for nid := range remotes {
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
	l.learnValueWithoutWrite(l.acceptor.acceptKey, l.acceptor.state.AccetpedValue)
	plog.Infof("learn value ok, while the instanceid %d, key %d, value %v", l.instanceID, l.acceptor.acceptKey, l.acceptor.state.AccetpedValue)
}
