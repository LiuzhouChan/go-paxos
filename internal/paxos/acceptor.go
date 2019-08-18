package ipaxos

import "github.com/LiuzhouChan/go-paxos/paxospb"

//Acceptor ...
type Acceptor struct {
	instance   IInstanceProxy
	instanceID uint64
	state      paxospb.AcceptorState
}

func (a *Acceptor) handlePrepare(msg paxospb.PaxosMsg) {

}

func (a *Acceptor) handleAccept(msg paxospb.PaxosMsg) {

}

//GetAcceptorState ...
func (a *Acceptor) GetAcceptorState() paxospb.AcceptorState {
	return a.state
}
