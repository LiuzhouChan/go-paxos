package paxos

import (
	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

var (
	plog = logger.GetLogger("paxos")
)

var (
	emptyState = paxospb.State{}
)

const (
	numMessageTypes uint64 = 25
)

type handlerFunc func(paxospb.PaxosMsg)
type setpFunc func(*instance, paxospb.PaxosMsg)

//Instance ...
type instance struct {
	nodeID     uint64
	applied    uint64
	instanceID uint64
	groupID    uint64
	log        *entryLog
	tickCount  uint64
	acceptor   *acceptor
	learner    *learner
	proposer   *proposer
	msgs       []paxospb.PaxosMsg

	remotes   map[uint64]*remote
	followers map[uint64]*remote
	handle    setpFunc
}

//NewInstance ...
func newInstance(c *config.Config, logdb ILogDB) *instance {
	if logdb == nil {
		panic("logdb is nil")
	}
	i := &instance{
		groupID:   c.GroupID,
		nodeID:    c.NodeID,
		msgs:      make([]paxospb.PaxosMsg, 0),
		log:       newEntryLog(logdb),
		remotes:   make(map[uint64]*remote),
		followers: make(map[uint64]*remote),
	}
	i.handle = defaultHandle
	return i
}

func (i *instance) resetForNewInstance() {

}

//Send ...
func (i *instance) send(msg paxospb.PaxosMsg) {
	msg.From = i.nodeID
	i.msgs = append(i.msgs, msg)
}

func (i *instance) addNode(nodeID uint64) {
	if _, ok := i.remotes[nodeID]; ok {
		//already a member
		return
	}
	//TODO: check whether it is a follower
	i.setRemote(nodeID, 0, 12)
}

func (i *instance) deleteRemote(nodeID uint64) {
	delete(i.remotes, nodeID)
}

func (i *instance) deleteFollower(nodeID uint64) {
	delete(i.followers, nodeID)
}

func (i *instance) setRemote(nodeID uint64, match uint64, next uint64) {
	plog.Infof("set remote, id %s, match %d, next %d", nodeID, match, next)
	i.remotes[nodeID] = &remote{
		next:  next,
		match: match,
	}
}

func (i *instance) paxosState() paxospb.State {
	return paxospb.State{
		Commit: i.log.committed,
	}
}

func (i *instance) loadState(st paxospb.State) {
	if st.Commit < i.log.committed || st.Commit > i.log.lastInstanceID() {
		plog.Panicf("got out of range state, st.commit %d, range[%d, %d]", st.Commit, i.log.committed, i.log.lastInstanceID())
	}
	i.log.committed = st.Commit
}

func (i *instance) Handle(msg paxospb.PaxosMsg) {
	i.handle(i, msg)
}

func (i *instance) handleLocalTick(msg paxospb.PaxosMsg) {
	i.tick()
}

func (i *instance) setApplied(applied uint64) {
	i.applied = applied
}

func (i *instance) getApplied() uint64 {
	return i.applied
}

func (i *instance) tick() {
	i.tickCount++
	i.proposer.tick()
	i.learner.tick()
}

func defaultHandle(i *instance, msg paxospb.PaxosMsg) {
	if msg.MsgType == paxospb.PaxosPrepareReply ||
		msg.MsgType == paxospb.PaxosAcceptReply ||
		msg.MsgType == paxospb.PaxosProposalSendNewValue {
		i.handleMessageForProposer(msg)
	} else if msg.MsgType == paxospb.PaxosPrepare ||
		msg.MsgType == paxospb.PaxosAccept {
		i.handleMessageForAcceptor(msg)
	} else if msg.MsgType == paxospb.PaxosLearnerAskForLearn ||
		msg.MsgType == paxospb.PaxosLearnerSendLearnValue ||
		msg.MsgType == paxospb.PaxosLearnerProposerSendSuccess ||
		msg.MsgType == paxospb.PaxosLearnerConfirmAskForLearn ||
		msg.MsgType == paxospb.PaxosLearnerSendNowInstanceID {
		i.handleMessageForLearner(msg)
	} else {
		plog.Errorf("Invalid msg type %v", msg.MsgType)
	}
}

func (i *instance) handleMessageForProposer(msg paxospb.PaxosMsg) {

}

func (i *instance) handleMessageForAcceptor(msg paxospb.PaxosMsg) {

}

func (i *instance) handleMessageForLearner(msg paxospb.PaxosMsg) {

}
