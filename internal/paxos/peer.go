package paxos

import (
	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/utils/stringutil"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

// PeerAddress is the basic info for a peer in the paxos group.
type PeerAddress struct {
	NodeID  uint64
	Address string
}

// Peer is the interface struct for interacting with the underlying Paxos
// protocol implementation.
type Peer struct {
	i         *instance
	prevState paxospb.State
}

// LaunchPeer starts or restarts a Paxos node.
func LaunchPeer(config *config.Config, logdb ILogDB,
	address []PeerAddress, initial bool, newNode bool) (*Peer, error) {
	i := newInstance(config, logdb)
	p := &Peer{i: i}
	_, lastInstance := logdb.GetRange()
	if lastInstance == 0 {
		p.prevState = emptyState
	} else {
		p.prevState = i.paxosState()
	}
	return p, nil
}

// Tick moves the logical clock forward by one tick.
func (p *Peer) Tick() {
	p.i.Handle(paxospb.PaxosMsg{
		MsgType:           paxospb.LocalTick,
		RejectByPromiseID: 0,
	})
}

// Propose ... where we need to a new instance
func (p *Peer) Propose(value []byte) {
	p.i.Handle(paxospb.PaxosMsg{
		MsgType: paxospb.Propose,
		From:    p.i.nodeID,
		Value:   stringutil.BytesDeepCopy(value),
	})
}

// Handle ...
func (p *Peer) Handle(msg paxospb.PaxosMsg) {
	_, rok := p.i.remotes[msg.From]
	if rok {
		p.i.Handle(msg)
	}
}

// HasUpdate returns a boolean value indicating whether there is any Update
// ready to be processed.
func (p *Peer) HasUpdate(moreEntriesToApply bool) bool {
	if pst := p.i.paxosState(); !paxospb.IsEmptyState(pst) &&
		!paxospb.IsStateEqual(pst, p.prevState) {
		// if it is not empty and not equal to pre
		return true
	}
	if len(p.i.msgs) > 0 {
		return true
	}
	if moreEntriesToApply && p.i.log.hasEntriesToApply() {
		return true
	}
	return false
}

// Commit commits the Update state to mark it as processed.
func (p *Peer) Commit(ud paxospb.Update) {
	p.i.msgs = nil
	if !paxospb.IsEmptyState(ud.State) {
		p.prevState = ud.State
	}
	p.i.log.commitUpdate(ud.UpdateCommit)
}

//NotifyPaxosLastApplied ...
func (p *Peer) NotifyPaxosLastApplied(lastApplied uint64) {
	p.i.setApplied(lastApplied)
}

// GetUpdate returns the current state of the Peer.
func (p *Peer) GetUpdate(moreEntriesToApply bool) paxospb.Update {
	return getUpdate(p.i, p.prevState, moreEntriesToApply)
}

func getUpdateCommit(ud paxospb.Update) paxospb.UpdateCommit {
	var uc paxospb.UpdateCommit
	if len(ud.CommittedEntries) > 0 {
		uc.AppliedTo = ud.CommittedEntries[len(ud.CommittedEntries)-1].AcceptorState.InstanceID
	}
	return uc
}

func getUpdate(i *instance, ppst paxospb.State,
	moreEntriesToApply bool) paxospb.Update {

	ud := paxospb.Update{
		GroupID:  i.groupID,
		NodeID:   i.nodeID,
		Messages: i.msgs,
	}

	if moreEntriesToApply {
		ud.CommittedEntries = i.log.getEntriesToApply()
	}

	if pst := i.paxosState(); !paxospb.IsStateEqual(pst, ppst) {
		ud.State = pst
	}
	ud.UpdateCommit = getUpdateCommit((ud))
	return ud
}
