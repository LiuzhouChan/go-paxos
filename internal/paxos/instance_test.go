package paxos

import "github.com/LiuzhouChan/go-paxos/paxospb"

type mockInstance struct {
	log     *entryLog
	nodeID  uint64
	msgs    []paxospb.PaxosMsg
	remotes map[uint64]*remote
}

func getTestInstance() IInstance {
	remotes := make(map[uint64]*remote, 0)
	for i := uint64(1); i < 5; i++ {
		remotes[i] = &remote{}
	}
	el := getTestEntryLog()
	ents := []paxospb.Entry{}
	for i := uint64(1); i < 11; i++ {
		ents = append(ents, paxospb.Entry{AcceptorState: paxospb.AcceptorState{InstanceID: i}})
	}
	el.append(ents)
	el.commitTo(el.lastInstanceID())
	mi := &mockInstance{
		remotes: remotes,
		log:     el,
		nodeID:  1,
		msgs:    make([]paxospb.PaxosMsg, 0),
	}
	return mi
}

func (mi *mockInstance) send(msg paxospb.PaxosMsg) {
	mi.msgs = append(mi.msgs, msg)
}

func (mi *mockInstance) getRemotes() map[uint64]*remote {
	return mi.remotes
}

func (mi *mockInstance) readMsgs() []paxospb.PaxosMsg {
	msgs := mi.msgs
	mi.msgs = make([]paxospb.PaxosMsg, 0)
	return msgs
}

func (mi *mockInstance) getNodeID() uint64 {
	return mi.nodeID
}

func (mi *mockInstance) getLog() *entryLog {
	return mi.log
}
