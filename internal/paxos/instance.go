package ipaxos

import (
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

var (
	plog = logger.GetLogger("paxos")
)

//IInstanceProxy ...
type IInstanceProxy interface {
	Send(msg paxospb.PaxosMsg)
}

//Instance ...
type Instance struct {
	acceptor *Acceptor
	learner  *Learner
	proposer *Proposer
	msgs     []paxospb.PaxosMsg
}

//NewInstance ...
func NewInstance() *Instance {
	return nil
}

func (i *Instance) initializeHandlerMap() {
	// acceptor

	// proposer

	// learner
}
