package paxos

import "github.com/LiuzhouChan/go-paxos/paxospb"

type nodeProxy struct {
	rn *node
}

func newNodeProxy(rn *node) *nodeProxy {
	return &nodeProxy{rn: rn}
}

func (n *nodeProxy) ApplyUpdate(ent paxospb.Entry,
	result uint64, rejected bool, ignored bool) {
	n.rn.applyUpdate(ent, result, rejected, ignored)
}

func (n *nodeProxy) NodeID() uint64 {
	return n.rn.nodeID
}

func (n *nodeProxy) GroupID() uint64 {
	return n.rn.groupID
}
