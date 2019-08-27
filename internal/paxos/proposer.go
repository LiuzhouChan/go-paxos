package ipaxos

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
	instance       IInstanceProxy
	prepareTimeout uint64
	acceptTimeout  uint64
	nodeID         uint64
	remote         map[uint64]bool

	proposalID                  uint64
	instanceID                  uint64
	highestOtherProposalID      uint64
	value                       []byte
	highestOtherPreAcceptBallot paxospb.BallotNumber
	canSkipPrepare              bool
	rejectBySomeone             bool
	votes                       map[uint64]bool
	tickCount                   uint64
	preparingTick               uint64
	acceptingTick               uint64
	st                          state
}

func newProposer() *proposer {
	p := &proposer{
		proposalID: 1,
		value:      []byte{},
	}
	return p
}

func (p *proposer) newInstance() {
	p.reset(p.instanceID + 1)
}

func (p *proposer) reset(instanceID uint64) {
	if p.instanceID != instanceID {
		p.proposalID = 1
	}
	p.instanceID = instanceID

	p.st = closing
	p.preparingTick = 0
	p.acceptingTick = 0

	p.highestOtherProposalID = 0
	p.highestOtherPreAcceptBallot = paxospb.BallotNumber{}
	p.canSkipPrepare = false
	p.rejectBySomeone = false
	p.value = p.value[:0]
	p.votes = make(map[uint64]bool)
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
	p.tickCount++
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
	return len(p.remote)/2 + 1
}

func (p *proposer) isSingleNodeQuorum() bool {
	return p.quorum() == 1
}

func (p *proposer) prepare(needNewBallot bool) {
	p.reset(p.instanceID)
	if needNewBallot {
		p.newPrepare()
	}
	msg := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosPrepare,
		InstanceID: p.instanceID,
		ProposalID: p.proposalID,
	}
	p.st = preparing
	p.preparingTick = 0
	for nid := range p.remote {
		msg.To = nid
		p.instance.Send(msg)
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

func (p *proposer) accept() {
	p.st = accepting
	p.acceptingTick = 0
	msg := paxospb.PaxosMsg{
		MsgType:    paxospb.PaxosAccept,
		InstanceID: p.instanceID,
		ProposalID: p.proposalID,
		Value:      stringutil.BytesDeepCopy(p.value),
	}

	for nid := range p.remote {
		msg.To = nid
		p.instance.Send(msg)
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
