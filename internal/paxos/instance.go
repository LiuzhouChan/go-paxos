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

//IInstance ...
type IInstance interface {
	send(paxospb.PaxosMsg)
	getRemotes() map[uint64]*remote
	readMsgs() []paxospb.PaxosMsg
	getNodeID() uint64
	getLog() *entryLog
}

type instance struct {
	instanceID uint64
	groupID    uint64
	nodeID     uint64
	log        *entryLog
	tickCount  uint64
	acceptor   *acceptor
	learner    *learner
	proposer   *proposer
	msgs       []paxospb.PaxosMsg

	applied uint64
	remotes map[uint64]*remote
	handle  setpFunc
}

//NewInstance ...
func newInstance(c *config.Config, logdb ILogDB) *instance {
	if logdb == nil {
		panic("logdb is nil")
	}
	i := &instance{
		groupID: c.GroupID,
		nodeID:  c.NodeID,
		msgs:    make([]paxospb.PaxosMsg, 0),
		log:     newEntryLog(logdb),
		remotes: make(map[uint64]*remote),
	}
	st := logdb.NodeState()
	if !paxospb.IsEmptyState(st) {
		i.loadState(st)
	}

	acceptor := newAcceptor(i)
	learner := newLearner(i, acceptor)
	proposer := newProposer(i, learner)

	acceptor.instanceID = i.instanceID
	acceptor.state = st.AcceptorState

	learner.instanceID = i.instanceID

	proposer.instanceID = i.instanceID
	proposer.proposalID = st.AcceptorState.PromiseBallot.ProposalID + 1

	i.acceptor = acceptor
	i.proposer = proposer
	i.learner = learner
	i.handle = defaultHandle
	return i
}

func (i *instance) resetForNewInstance() {
	i.acceptor.newInstance()
	i.learner.newInstance()
	i.proposer.newInstance()
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

	i.setRemote(nodeID, 0, 12)
}

func (i *instance) getRemotes() map[uint64]*remote {
	return i.remotes
}

func (i *instance) getNodeID() uint64 {
	return i.nodeID
}

func (i *instance) getLog() *entryLog {
	return i.log
}

func (i *instance) readMsgs() []paxospb.PaxosMsg {
	msgs := i.msgs
	i.msgs = make([]paxospb.PaxosMsg, 0)
	return msgs
}

func (i *instance) deleteRemote(nodeID uint64) {
	delete(i.remotes, nodeID)
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
		Commit:        i.log.committed,
		AcceptorState: i.acceptor.state,
	}
}

func (i *instance) loadState(st paxospb.State) {
	if st.Commit < i.log.committed || st.Commit > i.log.lastInstanceID() {
		plog.Panicf("got out of range state, st.commit %d, range[%d, %d]", st.Commit, i.log.committed, i.log.lastInstanceID())
	}
	i.log.committed = st.Commit
	i.instanceID = st.AcceptorState.InstanceID
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
	if msg.MsgType == paxospb.LocalTick {
		i.tick()
	} else if msg.MsgType == paxospb.Propose {
		i.handlePropose(msg)
	}
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

func (i *instance) handlePropose(msg paxospb.PaxosMsg) {
	if !i.learner.isIMLast() {
		plog.Warningf("instance handle propose, learner is not the last")
		return
	}
	i.proposer.newValue(msg.Key, msg.Value)
}

func (i *instance) handleMessageForProposer(msg paxospb.PaxosMsg) {
	if msg.InstanceID != i.proposer.instanceID {
		//Expired reply msg on last instance.
		//If the response of a node is always slower than the majority node,
		//then the message of the node is always ignored even if it is a reject reply.
		//In this case, if we do not deal with these reject reply, the node that
		//gave reject reply will always give reject reply.
		//This causes the node to remain in catch-up state.
		//
		//To avoid this problem, we need to deal with the expired reply.
		if msg.InstanceID+1 == i.proposer.instanceID {
			if msg.MsgType == paxospb.PaxosPrepareReply {
				i.proposer.handleExpiredPrepareReply(msg)
			} else if msg.MsgType == paxospb.PaxosAcceptReply {
				i.proposer.handleExpiredAcceptReply(msg)
			}
		}
		return
	}
	if msg.MsgType == paxospb.PaxosPrepareReply {
		i.proposer.handlePrepareReply(msg)
	} else if msg.MsgType == paxospb.PaxosAcceptReply {
		i.proposer.handleAcceptReply(msg)
	}
}

func (i *instance) handleMessageForAcceptor(msg paxospb.PaxosMsg) {
	if msg.InstanceID == i.acceptor.instanceID+1 {
		// skip success message
		m := msg
		msg.InstanceID = i.acceptor.instanceID
		m.MsgType = paxospb.PaxosLearnerProposerSendSuccess
		i.handleMessageForLearner(m)
		return
	}
	if msg.InstanceID == i.acceptor.instanceID {
		if msg.MsgType == paxospb.PaxosPrepare {
			i.acceptor.handlePrepare(msg)
			return
		}
		if msg.MsgType == paxospb.PaxosAccept {
			i.acceptor.handleAccept(msg)
		}
	}
}

func (i *instance) handleMessageForLearner(msg paxospb.PaxosMsg) {
	switch msg.MsgType {
	case paxospb.PaxosLearnerAskForLearn:
		i.learner.handleAskForLearn(msg)
	case paxospb.PaxosLearnerSendLearnValue:
		i.learner.handleSendLearnValue(msg)
	case paxospb.PaxosLearnerProposerSendSuccess:
		i.learner.handleProposerSendSuccess(msg)
	// case paxospb.PaxosLearnerSendNowInstanceID:
	// 	i.learner.handleSendNowInstanceID(msg)
	// case paxospb.PaxosLearnerConfirmAskForLearn:
	// 	i.learner.handleComfirmAskForLearn(msg)
	default:
		plog.Panicf("unknow msg type for learner")
	}

	// if we learned, commit it
	if i.learner.isLearned {
		ent := paxospb.Entry{
			Type:          paxospb.ApplicationEntry,
			Key:           msg.Key,
			AcceptorState: i.acceptor.state,
		}
		i.log.append([]paxospb.Entry{ent})
		i.log.commitTo(i.instanceID)
		i.resetForNewInstance()
	}
}
