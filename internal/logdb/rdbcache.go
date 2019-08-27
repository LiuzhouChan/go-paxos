package logdb

import (
	"sync"

	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

type rdbcache struct {
	nodeInfo       map[paxosio.NodeInfo]struct{}
	ps             map[paxosio.NodeInfo]paxospb.AcceptorState
	lastEntryBatch map[paxosio.NodeInfo]paxospb.EntryBatch
	maxInstance    map[paxosio.NodeInfo]uint64
	mu             sync.Mutex
}

func newRDBCache() *rdbcache {
	return &rdbcache{
		nodeInfo:       make(map[paxosio.NodeInfo]struct{}),
		ps:             make(map[paxosio.NodeInfo]paxospb.AcceptorState),
		lastEntryBatch: make(map[paxosio.NodeInfo]paxospb.EntryBatch),
		maxInstance:    make(map[paxosio.NodeInfo]uint64),
	}
}

func (r *rdbcache) setNodeInfo(groupID uint64, nodeID uint64) bool {
	key := paxosio.NodeInfo{GroupID: groupID, NodeID: nodeID}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.nodeInfo[key]
	if !ok {
		r.nodeInfo[key] = struct{}{}
	}
	return !ok
}

func (r *rdbcache) setState(groupID uint64,
	nodeID uint64, st paxospb.AcceptorState) bool {
	key := paxosio.NodeInfo{GroupID: groupID, NodeID: nodeID}
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.ps[key]
	if !ok {
		r.ps[key] = st
		return true
	}
	if paxospb.IsAcceptorStateEqual(v, st) {
		return false
	}
	r.ps[key] = st
	return true
}

func (r *rdbcache) setMaxInstance(groupID uint64,
	nodeID uint64, maxInstance uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := paxosio.NodeInfo{GroupID: groupID, NodeID: nodeID}
	r.maxInstance[key] = maxInstance
}

func (r *rdbcache) getMaxInstance(groupID uint64,
	nodeID uint64) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := paxosio.NodeInfo{GroupID: groupID, NodeID: nodeID}
	v, ok := r.maxInstance[key]
	if !ok {
		return 0, false
	}
	return v, true
}

func (r *rdbcache) setLastEntryBatch(groupID uint64,
	nodeID uint64, eb paxospb.EntryBatch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := paxosio.NodeInfo{GroupID: groupID, NodeID: nodeID}
	oeb, ok := r.lastEntryBatch[key]
	if !ok {
		oeb = paxospb.EntryBatch{Entries: make([]paxospb.Entry, 0, len(eb.Entries))}
	} else {
		oeb.Entries = oeb.Entries[:0]
	}
	oeb.Entries = append(oeb.Entries, eb.Entries...)
	r.lastEntryBatch[key] = oeb
}

func (r *rdbcache) getLastEntryBatch(groupID uint64,
	nodeID uint64, lb paxospb.EntryBatch) (paxospb.EntryBatch, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := paxosio.NodeInfo{GroupID: groupID, NodeID: nodeID}
	v, ok := r.lastEntryBatch[key]
	if !ok {
		return v, false
	}
	lb.Entries = lb.Entries[:0]
	lb.Entries = append(lb.Entries, v.Entries...)
	return lb, true
}
