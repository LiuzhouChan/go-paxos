package paxos

import (
	"github.com/LiuzhouChan/go-paxos/internal/utils/stringutil"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

type state uint64

const (
	preparing state = iota
	accepting
	closing
)

var stateNames = [...]string{
	"Preparing",
	"Accepting",
	"Closing",
}

//String ....
func (st state) String() string {
	return stateNames[st]
}

//proposer ...
type proposer struct {
	instance                    *instance
	nodeID                      uint64
	proposalID                  uint64
	instanceID                  uint64
	highestOtherProposalID      uint64
	value                       []byte
	highestOtherPreAcceptBallot paxospb.BallotNumber
	canSkipPrepare              bool
	rejectBySomeone             bool
	votes                       map[uint64]bool
	preparingTick               uint64
	acceptingTick               uint64
	prepareTimeout              uint64
	acceptTimeout               uint64
	st                          state
	learner                     *learner
}

func newProposer(i *instance) *proposer {
	p := &proposer{
		instance:   i,
		proposalID: 1,
		value:      []byte{},
	}
	return p
}

func (p *proposer) newInstance() {
	p.instanceID++
	p.votes = make(map[uint64]bool)
	p.highestOtherProposalID = 0
	p.value = p.value[:0]
	p.st = closing
}

func (p *proposer) newPrepare() {
	maxProposalID := p.highestOtherProposalID
	if p.proposalID >= maxProposalID {
		maxProposalID = p.proposalID
	}
	p.proposalID = maxProposalID + 1
}

func (p *proposer) addPreAcceptValue(ob paxospb.BallotNumber,
	ov []byte) {
	if ob.IsNil() {
		return
	}
	if !p.highestOtherPreAcceptBallot.IsNotLessThan(ob) {
		p.highestOtherPreAcceptBallot = ob
		p.value = stringutil.BytesDeepCopy(ov)
	}
}

func (p *proposer) setOtherProposalID(op uint64) {
	if op > p.highestOtherProposalID {
		p.highestOtherProposalID = op
	}
}

func (p *proposer) tick() {
	if p.st == preparing {
		p.prepareTick()
		if p.timeForPrepareTimeout() {
			p.prepare(p.rejectBySomeone)
		}
	} else if p.st == accepting {
		p.acceptTick()
		if p.timeForAcceptTimeout() {
			p.prepare(p.rejectBySomeone)
		}
	}
}

func (p *proposer) prepareTick() {
	p.preparingTick++
}

func (p *proposer) acceptTick() {
	p.acceptingTick++
}

func (p *proposer) timeForPrepareTimeout() bool {
	return p.preparingTick >= p.prepareTimeout
}

func (p *proposer) timeForAcceptTimeout() bool {
	return p.acceptingTick >= p.acceptTimeout
}

func (p *proposer) quorum() int {
	return len(p.instance.remotes)/2 + 1
}

func (p *proposer) isSingleNodeQuorum() bool {
	return p.quorum() == 1
}

func (p *proposer) newValue(value []byte) {
	if len(p.value) == 0 {
		p.value = stringutil.BytesDeepCopy(value)
	}
	// set timeout ddl
	if p.canSkipPrepare && !p.rejectBySomeone {
		plog.Infof("skip prepare, direct start accept")
		p.accept()
	} else {
		p.prepare(p.rejectBySomeone)
	}
}

func (p *proposer) prepare(needNewBallot bool) {
	p.st = preparing
	p.preparingTick = 0
	p.canSkipPrepare = false
	p.rejectBySomeone = false
	p.highestOtherPreAcceptBallot = paxospb.BallotNumber{}
	if needNewBallot {
		p.newPrepare()
	}
	msg := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosPrepare,
		InstanceID: p.instanceID,
		ProposalID: p.proposalID,
	}
	p.votes = make(map[uint64]bool)
	for nid := range p.instance.remotes {
		msg.To = nid
		p.instance.send(msg)
	}
}

func (p *proposer) handlePrepareResp(msg paxospb.PaxosMsg) {
	if p.st != preparing {
		return
	}
	if p.proposalID != msg.ProposalID {
		return
	}
	if msg.RejectByPromiseID == 0 {
		b := paxospb.BallotNumber{
			ProposalID: msg.PreAcceptID,
			NodeID:     msg.PreAcceptNodeID,
		}
		p.addPreAcceptValue(b, msg.Value)
	} else {
		p.rejectBySomeone = true
		p.setOtherProposalID(msg.RejectByPromiseID)
	}
	count := 0
	if _, ok := p.votes[msg.From]; !ok {
		p.votes[msg.From] = msg.RejectByPromiseID == 0
	}
	for _, v := range p.votes {
		if v {
			count++
		}
	}
	if count == p.quorum() {
		// pass
		plog.Infof("[Pass] start accept")
		p.canSkipPrepare = true
		p.votes = make(map[uint64]bool)
		p.accept()
	} else if len(p.votes)-count == p.quorum() {
		// if the reject is maj, wait for 30ms and restart prepare
	}
}

func (p *proposer) handleExpiredPrepareReply(msg paxospb.PaxosMsg) {
	if msg.RejectByPromiseID != 0 {
		plog.Infof("[expired prepare reply] reject by promise id %v", msg.RejectByPromiseID)
		p.rejectBySomeone = true
		p.setOtherProposalID(msg.RejectByPromiseID)
	}
}

func (p *proposer) accept() {
	p.st = accepting
	p.acceptingTick = 0
	msg := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosAccept,
		InstanceID: p.instanceID,
		ProposalID: p.proposalID,
		Value:      stringutil.BytesDeepCopy(p.value),
	}
	p.votes = make(map[uint64]bool)
	for nid := range p.instance.remotes {
		msg.To = nid
		p.instance.send(msg)
	}
}

func (p *proposer) handleAcceptResp(msg paxospb.PaxosMsg) {
	if p.st != accepting {
		return
	}
	if msg.ProposalID != p.proposalID {
		return
	}
	if msg.RejectByPromiseID != 0 {
		//reject
		p.rejectBySomeone = true
		p.setOtherProposalID(msg.RejectByPromiseID)
	}
	count := 0
	if _, ok := p.votes[msg.From]; !ok {
		p.votes[msg.From] = msg.RejectByPromiseID == 0
	}
	for _, v := range p.votes {
		if v {
			count++
		}
	}

	if count == p.quorum() {
		// pass
		plog.Infof("[Pass] start send learn")
		p.st = closing

	} else if len(p.votes)-count == p.quorum() {
		plog.Infof("[Not Pass] wait and restart prepare")
	}
}

func (p *proposer) handleExpiredAcceptReply(msg paxospb.PaxosMsg) {
	if msg.RejectByPromiseID != 0 {
		plog.Infof("[expired accept reply reject] reject by promiseID %v", msg.RejectByPromiseID)
		p.rejectBySomeone = true
		p.setOtherProposalID(msg.RejectByPromiseID)
	}
}
