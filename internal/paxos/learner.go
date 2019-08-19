package ipaxos

//Learner ...
type Learner struct {
	instance     IInstanceProxy
	instanceID   uint64
	isLearned    bool
	learnedValue string
	acceptor     *Acceptor
}
