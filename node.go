package paxos

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/logdb"
	ipaxos "github.com/LiuzhouChan/go-paxos/internal/paxos"
	"github.com/LiuzhouChan/go-paxos/internal/rsm"
	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/internal/transport"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

var (
	incomingProposalsMaxLen = settings.Soft.IncomingProposalQueueLength
	lazyFreeCycle           = settings.Soft.LazyFreeCycle
)

type node struct {
	paxosAddress        string
	config              config.Config
	groupID             uint64
	nodeID              uint64
	commitC             chan<- rsm.Commit
	mq                  *server.MessageQueue
	lastApplied         uint64
	commitReady         func(uint64)
	sendPaxosMessage    func(paxospb.PaxosMsg)
	sm                  *rsm.StateMachine
	incomingProposals   *entryQueue
	pendingProposals    *pendingProposal
	publishedInstanceID uint64
	paxosMu             sync.Mutex
	node                *ipaxos.Peer
	logreader           *logdb.LogReader
	logdb               paxosio.ILogDB
	nodeRegistry        transport.INodeRegistry
	stopc               chan struct{}
	groupInfo           atomic.Value
	tickCount           uint64
	expireNotified      uint64
	closeOnece          sync.Once
	initializedMu       struct {
		sync.Mutex
		initialized bool
	}
}

func newNode(paxosAddress string,
	peers map[uint64]string,
	initialMember bool,
	dataStore rsm.IManagedStateMachine,
	commitReady func(uint64),
	sendMessage func(paxospb.PaxosMsg),
	mq *server.MessageQueue,
	stopc chan struct{},
	nodeRegistry transport.INodeRegistry,
	requestStatePool *sync.Pool,
	config config.Config,
	tickMillisecond uint64,
	ldb paxosio.ILogDB) *node {
	proposals := newEntryQueue(incomingProposalsMaxLen)
	pp := newPendingProposal(requestStatePool, proposals,
		config.GroupID, config.NodeID, paxosAddress, tickMillisecond)
	lr := logdb.NewLogReader(config.GroupID, config.NodeID, ldb)
	rc := &node{
		config:            config,
		paxosAddress:      paxosAddress,
		incomingProposals: proposals,
		commitReady:       commitReady,
		stopc:             stopc,
		pendingProposals:  pp,
		nodeRegistry:      nodeRegistry,
		logreader:         lr,
		sendPaxosMessage:  sendMessage,
		mq:                mq,
		logdb:             ldb,
		groupID:           config.GroupID,
		nodeID:            config.NodeID,
	}
	nodeProxy := newNodeProxy(rc)
	sm := rsm.NewStateMachine(dataStore, nodeProxy)
	rc.commitC = sm.CommitC()
	rc.sm = sm
	rc.startPaxos(config, rc.logreader, peers, initialMember)
	return rc
}

func (rc *node) startPaxos(cc config.Config,
	logdb ipaxos.ILogDB, peers map[uint64]string, initial bool) {
	newNode := rc.replayLog(cc.GroupID, cc.NodeID)
	pas := make([]ipaxos.PeerAddress, 0)
	for k, v := range peers {
		pas = append(pas, ipaxos.PeerAddress{NodeID: k, Address: v})
	}
	node, err := ipaxos.LaunchPeer(&cc, logdb, pas, initial, newNode)
	if err != nil {
		panic(err)
	}
	rc.node = node
}

func (rc *node) close() {
	rc.requestRemoval()
	rc.pendingProposals.close()
}

func (rc *node) stopped() bool {
	select {
	case <-rc.stopc:
		return true
	default:
	}
	return false
}

func (rc *node) requestRemoval() {
	rc.closeOnece.Do(func() {
		close(rc.stopc)
	})
	plog.Infof("%s called requestRemoval()", rc.describe())
}

func (rc *node) shouldStop() <-chan struct{} {
	return rc.stopc
}

func (rc *node) propose(cmd []byte, handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	return rc.pendingProposals.propose(cmd, handler, timeout)
}

func (rc *node) describe() string {
	return logger.DescribeNode(rc.groupID, rc.nodeID)
}

func (rc *node) notifyOffloaded(from rsm.From) {
	rc.sm.Offloaded(from)
}

func (rc *node) notifyLoaded(from rsm.From) {
	rc.sm.Loaded(from)
}

func (rc *node) entriesToApply(ents []paxospb.Entry) (nents []paxospb.Entry) {
	if len(ents) == 0 {
		return
	}
	if rc.stopped() {
		return
	}
	// last instance id of ents
	li := ents[len(ents)-1].AcceptorState.InstanceID
	if li < rc.publishedInstanceID {
		plog.Panicf("%s got entries [%d-%d] older than current %d", rc.describe(),
			ents[0].AcceptorState.InstanceID, li, rc.publishedInstanceID)
	}
	fi := ents[0].AcceptorState.InstanceID
	if fi > rc.publishedInstanceID+1 {
		plog.Panicf("%s has hole in to be applied logs, found %d, want %d",
			rc.describe(), fi, rc.publishedInstanceID)
	}
	if rc.publishedInstanceID-fi+1 < uint64(len(ents)) {
		nents = ents[rc.publishedInstanceID-fi+1:]
	}
	return
}

func (rc *node) publishCommitRec(rec rsm.Commit) bool {
	if rc.stopped() {
		return false
	}
	select {
	case rc.commitC <- rec:
		rc.commitReady(rc.groupID)
	case <-rc.stopc:
		return false
	}
	return true
}

func (rc *node) publishEntries(ents []paxospb.Entry) bool {
	if len(ents) == 0 {
		return true
	}
	rec := rsm.Commit{
		Entries: ents,
	}
	if !rc.publishCommitRec(rec) {
		return false
	}
	plog.Infof("the length of ents is %d", len(ents))
	rc.publishedInstanceID = ents[len(ents)-1].AcceptorState.InstanceID
	return true
}

func (rc *node) replayLog(groupID uint64, nodeID uint64) bool {
	plog.Infof("%s is replaying logs", rc.describe())
	ps, err := rc.logdb.ReadPaxosState(groupID, nodeID, 0)
	if err == paxosio.ErrNoSavedLog {
		return true
	}
	if err != nil {
		panic(err)
	}
	if ps.State != nil {
		plog.Infof("%s logdb ents entryCount %d commit %d",
			rc.describe(), ps.EntryCount, ps.State.Commit)
		rc.logreader.SetState(*ps.State)
	}
	rc.logreader.SetRange(ps.FirstInstanceID, ps.EntryCount)
	newNode := true
	if ps.EntryCount > 0 || ps.State != nil {
		newNode = false
	}
	return newNode
}

func (rc *node) handleCommit(batch []rsm.Commit) (rsm.Commit, bool) {
	return rc.sm.Handle(batch)
}

func (rc *node) sendMessages(msgs []paxospb.PaxosMsg) {
	for _, msg := range msgs {
		msg.GroupID = rc.groupID
		rc.sendPaxosMessage(msg)
	}
}

func (rc *node) getUpdate() (paxospb.Update, bool) {
	moreEntriesToApply := rc.canHaveMoreEntriesToApply()
	if rc.node.HasUpdate(moreEntriesToApply) {
		ud := rc.node.GetUpdate(moreEntriesToApply)
		for iid := range ud.Messages {
			ud.Messages[iid].GroupID = rc.groupID
		}
		ud.LastApplied = rc.lastApplied
		return ud, true
	}
	return paxospb.Update{}, false
}

func (rc *node) canHaveMoreEntriesToApply() bool {
	return uint64(cap(rc.commitC)-len(rc.commitC)) > 0
}

func (rc *node) applyPaxosUpdates(ud paxospb.Update) bool {
	toApply := rc.entriesToApply(ud.CommittedEntries)
	if ok := rc.publishEntries(toApply); !ok {
		return false
	}
	return true
}

func (rc *node) processPaxosUpdate(ud paxospb.Update) bool {
	rc.logreader.Append(ud.EntriesToSave)
	rc.sendMessages(ud.Messages)
	return true
}

func (rc *node) commitPaxosUpdate(ud paxospb.Update) {
	rc.paxosMu.Lock()
	rc.node.Commit(ud)
	rc.paxosMu.Unlock()
}

func (rc *node) updateLastApplied() uint64 {
	rc.lastApplied = rc.sm.GetLastApplied()
	rc.node.NotifyPaxosLastApplied(rc.lastApplied)
	return rc.lastApplied
}

func (rc *node) stepNode() (paxospb.Update, bool) {
	rc.paxosMu.Lock()
	defer rc.paxosMu.Unlock()
	if rc.initialized() {
		if rc.handleEvents() {
			return rc.getUpdate()
		}
	}
	return paxospb.Update{}, false
}

func (rc *node) handleEvents() bool {
	hasEvent := false
	if rc.handleReceiveMessages() {
		hasEvent = true
	}
	if rc.handleProposals() {
		hasEvent = true
	}
	if hasEvent {
		if rc.expireNotified != rc.tickCount {
			rc.pendingProposals.gc()
			rc.expireNotified = rc.tickCount
		}
	}
	return hasEvent
}

func (rc *node) handleProposals() bool {
	ent, ok := rc.incomingProposals.get()
	if !ok {
		return false
	}
	rc.node.Propose(ent.Key, ent.AcceptorState.AccetpedValue)
	return true
}

func (rc *node) handleLocalTickMessage(count uint64) {
	for i := uint64(0); i < count; i++ {
		rc.tick()
	}
}

func (rc *node) tick() {
	if rc.node == nil {
		panic("rc node is still nil")
	}
	rc.tickCount++
	rc.node.Tick()
	rc.pendingProposals.increaseTick()
}

func (rc *node) handleReceiveMessages() bool {
	hasEvent := false
	ltCount := uint64(0)
	msgs := rc.mq.Get()
	for _, m := range msgs {
		hasEvent = true
		if m.MsgType == paxospb.LocalTick {
			ltCount++
			continue
		}
		if done := rc.handleMessage(m); !done {
			if m.GroupID != rc.groupID {
				plog.Panicf("received messages for group %d on %d", m.GroupID, rc.groupID)
			}
			rc.node.Handle(m)
		}
	}
	rc.handleLocalTickMessage(ltCount)
	return hasEvent
}

func (rc *node) handleMessage(msg paxospb.PaxosMsg) bool {
	switch msg.MsgType {
	case paxospb.LocalTick:
		rc.tick()
	default:
		return false
	}
	return true
}

func (rc *node) applyUpdate(entry paxospb.Entry,
	result uint64, rejected bool, ignored bool) {
	if !ignored {
		if entry.Key == 0 {
			plog.Panicf("key is 0")
		}
		rc.pendingProposals.applied(entry.Key, result, rejected)
	}
}

func (rc *node) initialized() bool {
	rc.initializedMu.Lock()
	defer rc.initializedMu.Unlock()
	return rc.initializedMu.initialized
}

func (rc *node) setInitialized() {
	rc.initializedMu.Lock()
	defer rc.initializedMu.Unlock()
	rc.initializedMu.initialized = true
}

func (rc *node) setInitialStatus(instanceID uint64) {
	if rc.initialized() {
		panic("setInitialStatus called twice")
	}
	plog.Infof("%s initial instance id set to %d", rc.describe(), instanceID)
	rc.publishedInstanceID = instanceID
	rc.setInitialized()
}
