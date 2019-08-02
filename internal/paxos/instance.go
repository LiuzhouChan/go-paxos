package ipaxos

type Instance struct {
	acceptor *Acceptor
	learner  *Learner
	proposer *Proposer
}

func NewInstance() *Instance {
	return nil
}
