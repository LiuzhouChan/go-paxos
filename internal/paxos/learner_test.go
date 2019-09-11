package paxos

func getTestLearner() *learner {
	acceptor := getTestAcceptor()
	l := newLearner(acceptor.instance, acceptor)
	return l
}
