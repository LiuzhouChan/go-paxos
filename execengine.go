package paxos

import (
	"time"

	"github.com/LiuzhouChan/go-paxos/internal/rsm"
	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/internal/utils/syncutil"
)

var (
	workerCount        = settings.Hard.StepEngineWorkerCount
	commitWorkerCount  = settings.Soft.StepEngineCommitWorkerCount
	nodeReloadInterval = time.Millisecond * time.Duration(settings.Soft.NodeReloadMillisecond)
	commitBatchSize    = 10
)

type nodeLoader interface {
	getGroupSetIndex() uint64
	forEachGroupRun(bf func() bool,
		af func() bool, f func(uint64, *node) bool)
}

type workReady struct {
	partitioner  server.IPartitioner
	count        uint64
	readyMapList []*readyGroup
	readyChList  []chan uint64
}

func newWorkReady(count uint64) *workReady {
	wr := &workReady{
		partitioner:  server.NewFixedPartitioner(count),
		count:        count,
		readyMapList: make([]*readyGroup, count),
		readyChList:  make([]chan uint64, count),
	}
	for i := uint64(0); i < count; i++ {
		wr.readyChList[i] = make(chan uint64, 1)
		wr.readyMapList[i] = newReadyGroup()
	}
	return wr
}

func (wr *workReady) getPartitioner() server.IPartitioner {
	return wr.partitioner
}

func (wr *workReady) groupReady(groupID uint64) {
	idx := wr.partitioner.GetPartitionID(groupID)
	readyMap := wr.readyMapList[idx]
	readyMap.setGroupReady(groupID)
	select {
	case wr.readyChList[idx] <- groupID:
	default:
	}
}

func (wr *workReady) waitCh(workerID uint64) chan uint64 {
	return wr.readyChList[workerID-1]
}

func (wr *workReady) getReadyMap(workerID uint64) map[uint64]struct{} {
	readyMap := wr.readyMapList[workerID-1]
	return readyMap.getReadyGroups()
}

type sendLocalMessageFunc func(groupID uint64, nodeID uint64)

type execEngine struct {
	nodeStopper     *syncutil.Stopper
	commitStopper   *syncutil.Stopper
	nh              nodeLoader
	ctx             *server.Context
	logdb           paxosio.ILogDB
	ctxs            []paxosio.IContext
	profilers       []*profiler
	nodeWorkReady   *workReady
	commitWorkReady *workReady
	sendLocalMsg    sendLocalMessageFunc
}

func newExecEngine(nh nodeLoader, ctx *server.Context,
	logdb paxosio.ILogDB, sendLocalMsg sendLocalMessageFunc) *execEngine {
	s := &execEngine{
		nh:              nh,
		ctx:             ctx,
		logdb:           logdb,
		nodeStopper:     syncutil.NewStopper(),
		commitStopper:   syncutil.NewStopper(),
		nodeWorkReady:   newWorkReady(workerCount),
		commitWorkReady: newWorkReady(commitWorkerCount),
		ctxs:            make([]paxosio.IContext, workerCount),
		profilers:       make([]*profiler, workerCount),
		sendLocalMsg:    sendLocalMsg,
	}
	sampleRatio := int64(delaySampleRatio) / 10
	for i := uint64(1); i <= workerCount; i++ {
		workerID := i
		s.ctxs[i-1] = logdb.GetLogDBThreadContext()
		s.profilers[i-1] = newProfiler(sampleRatio)
		s.nodeStopper.RunWorker(func() {
			s.nodeWorkMain(workerID)
		})
	}
	for i := uint64(1); i <= commitWorkerCount; i++ {
		commitWorkerID := i
		s.commitStopper.RunWorker(func() {
			s.commitWorkerMain(commitWorkerID)
		})
	}
	return s
}

func (s *execEngine) stop() {
	s.nodeStopper.Stop()
	s.commitStopper.Stop()
	for _, ctx := range s.ctxs {
		if ctx != nil {
			ctx.Destroy()
		}
	}
	s.logProfileStats()
}

func (s *execEngine) logProfileStats() {
	for _, p := range s.profilers {
		if p.ratio == 0 {
			continue
		}
		plog.Infof("execEngine logProfileStats")
	}
}

func (s *execEngine) commitWorkerMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	batch := make([]rsm.Commit, 0, commitBatchSize)
	cci := uint64(0)
	for {
		select {
		case <-s.commitStopper.ShouldStop():
			s.offloadNodeMap(nodes, rsm.FromCommitWorker)
			return
		case <-ticker.C:
			nodes, cci = s.loadSMs(workerID, cci, nodes)
			s.execSMs(workerID, make(map[uint64]struct{}), nodes, batch)
			batch = make([]rsm.Commit, 0, commitBatchSize)
		case <-s.commitWorkReady.waitCh(workerID):
			clusterIDMap := s.commitWorkReady.getReadyMap(workerID)
			s.execSMs(workerID, clusterIDMap, nodes, batch)
		}
	}
}

func (s *execEngine) nodeWorkMain(workerID uint64) {
	nodes := make(map[uint64]*node)
	ticker := time.NewTicker(nodeReloadInterval)
	defer ticker.Stop()
	cci := uint64(0)
	stopC := s.nodeStopper.ShouldStop()
	for {
		select {
		case <-stopC:
			s.offloadNodeMap(nodes, rsm.FromStepWorker)
			return
		case <-ticker.C:
			nodes, cci = s.loadNodes(workerID, cci, nodes)
			s.execNodes(workerID, make(map[uint64]struct{}), nodes, stopC)
		case <-s.nodeWorkReady.waitCh(workerID):
			groupIDMap := s.nodeWorkReady.getReadyMap(workerID)
			s.execNodes(workerID, groupIDMap, nodes, stopC)
		}
	}
}

func (s *execEngine) loadSMs(workerID uint64, cci uint64,
	nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return s.loadBucketNodes(workerID, cci, nodes,
		s.commitWorkReady.getPartitioner(), rsm.FromCommitWorker)
}

func (s *execEngine) execSMs(wokerID uint64,
	idmap map[uint64]struct{},
	nodes map[uint64]*node, batch []rsm.Commit) bool {
	if len(idmap) == 0 {
		for k := range nodes {
			idmap[k] = struct{}{}
		}
	}
	var p *profiler
	if workerCount == commitWorkerCount {
		p = s.profilers[wokerID-1]
		p.newCommitIteration()
		p.exec.start()
	}
	hasEvent := false
	for groupID := range idmap {
		node, ok := nodes[groupID]
		if !ok || node.stopped() {
			continue
		}
		node.handleCommit(batch)
	}
	if p != nil {
		p.exec.end()
	}
	return hasEvent
}

func (s *execEngine) loadNodes(workerID uint64,
	cci uint64, nodes map[uint64]*node) (map[uint64]*node, uint64) {
	return s.loadBucketNodes(workerID, cci, nodes,
		s.nodeWorkReady.getPartitioner(), rsm.FromStepWorker)
}

// load the new node set when the node set is changed
func (s *execEngine) loadBucketNodes(workerID uint64,
	cci uint64, nodes map[uint64]*node, partitioner server.IPartitioner,
	from rsm.From) (map[uint64]*node, uint64) {
	bucket := workerID - 1
	newCCI := s.nh.getGroupSetIndex()
	if newCCI != cci {
		newNodes := make(map[uint64]*node)
		s.nh.forEachGroupRun(nil,
			func() bool {
				for cid, node := range nodes {
					_, ok := newNodes[cid]
					if !ok {
						node.notifyOffloaded(from)
					}
				}
				return true
			},
			func(cid uint64, v *node) bool {
				if partitioner.GetPartitionID(cid) == bucket {
					v.notifyLoaded(from)
					newNodes[cid] = v
				}
				return true
			})
		return newNodes, newCCI
	}
	return nodes, cci
}

func (s *execEngine) execNodes(workerID uint64,
	groupIDMap map[uint64]struct{},
	nodes map[uint64]*node, stopC chan struct{}) {
	if len(nodes) == 0 {
		return
	}
	nodeCtx := s.ctxs[workerID-1]
	nodeCtx.Reset()
	p := s.profilers[workerID-1]
	p.newIteration()
	p.step.start()
	if len(groupIDMap) == 0 {
		for cid := range nodes {
			groupIDMap[cid] = struct{}{}
		}
	}
	nodeUpdates := nodeCtx.GetUpdates()
	for cid := range groupIDMap {
		node, ok := nodes[cid]
		if !ok {
			continue
		}
		ud, hasUpdate := node.stepNode()
		if hasUpdate {
			nodeUpdates = append(nodeUpdates, ud)
		}
	}
	s.applyUpdate(nodeUpdates, nodes)
	// send msg now
	for _, ud := range nodeUpdates {
		node := nodes[ud.GroupID]
		node.sendMessages(ud.Messages)
	}
	p.step.end()
	p.recordEntryCount(nodeUpdates)
	p.save.start()
	if err := s.logdb.SavePaxosState(nodeUpdates, nodeCtx); err != nil {
		panic(err)
	}
	p.save.end()
	p.cs.start()
	for _, ud := range nodeUpdates {
		node := nodes[ud.GroupID]
		if !node.processPaxosUpdate(ud) {
			plog.Infof("process update failed, ready to exit")
		}
		node.commitPaxosUpdate(ud)
	}
	p.cs.end()
}

func resetNodeUpdate(ud []paxospb.Update) {
	for i := range ud {
		ud[i].EntriesToSave = nil
		ud[i].CommittedEntries = nil
		for j := range ud[i].Messages {
			ud[i].Messages[j].Value = nil
		}
	}
}

func (s *execEngine) applyUpdate(updates []paxospb.Update,
	nodes map[uint64]*node) {
	for _, ud := range updates {
		node := nodes[ud.GroupID]
		if !node.applyPaxosUpdates(ud) {
			plog.Infof("paxos update not publish")
		}
	}
}

func (s *execEngine) setNodeReady(groupID uint64) {
	s.nodeWorkReady.groupReady(groupID)
}

func (s *execEngine) SetCommitReady(groupID uint64) {
	s.commitWorkReady.groupReady(groupID)
}

func (s *execEngine) offloadNodeMap(nodes map[uint64]*node,
	from rsm.From) {
	for _, node := range nodes {
		node.notifyOffloaded(from)
	}
}
