package paxos

import (
	"time"

	"github.com/LiuzhouChan/go-paxos/internal/server"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/internal/utils/syncutil"
	"github.com/sirupsen/logrus"
)

var (
	workerCount        = settings.Hard.StepEngineWorkerCount
	commitWorkerCount  = settings.Soft.StepEngineCommitWorkerCount
	nodeReloadInterval = time.Millisecond * time.Duration(settings.Soft.NodeReloadMillisecond)
	// commitBatchSize    = settings.Soft.CommitBatchSize
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

type execEngine struct {
	nodeStopper   *syncutil.Stopper
	commitStopper *syncutil.Stopper
}

func (s *execEngine) logProfileStats() {
	logrus.Info("prop")

}
