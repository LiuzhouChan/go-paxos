package ipaxos

import (
	pb "github.com/LiuzhouChan/go-paxos/paxospb"
)

type Acceptor struct {
	instanceID uint64
	state      pb.AcceptorState
}

func (a *Acceptor) handlePrepare(msg pb.PaxosMsg) {

}

func (a *Acceptor) handleAccept(msg pb.PaxosMsg) {

}

func (a *Acceptor) GetAcceptorState() pb.AcceptorState {
	return a.state
}
