package main

import (
	"fmt"
	"os"
	"time"

	"github.com/LiuzhouChan/go-paxos"
	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/statemachine"
)

const (
	dataDirectoryName = "checkdisk-data-safe-to-delete"
)

type dummyStateMachine struct {
}

func newDummyStateMachine(groupID uint64, nodeID uint64) statemachine.IStateMachine {
	return &dummyStateMachine{}
}

func (s *dummyStateMachine) Lookup(query []byte) []byte {
	return query
}

func (s *dummyStateMachine) Update(data []byte) uint64 {
	return 1
}

func (s *dummyStateMachine) Close() {}

func (s *dummyStateMachine) GetHash() uint64 { return 1 }

func main() {
	os.RemoveAll(dataDirectoryName)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("logdb").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("paxos").SetLevel(logger.WARNING)
	logger.GetLogger("go-paxos").SetLevel(logger.WARNING)

	nhc := config.NodeHostConfig{
		NodeHostDir:    dataDirectoryName,
		RTTMillisecond: 200,
		PaxosAddress:   "localhost:26000",
	}
	nh := paxos.NewNodeHost(nhc)
	defer nh.Stop()
	rc := config.Config{
		GroupID:        1,
		NodeID:         1,
		AskForLearnRTT: 20,
	}
	nodes := make(map[uint64]string)
	nodes[1] = nhc.PaxosAddress
	for i := uint64(1); i <= uint64(48); i++ {
		rc.GroupID = i
		if err := nh.StartGroup(nodes, false, newDummyStateMachine, rc); err != nil {
			panic(err)
		}
	}
	time.Sleep(time.Millisecond * 200)
	fmt.Printf("all clusters are ready, will keep making proposals for 60 seconds\n")
	doneCh := make(chan struct{}, 1)
	timer := time.NewTimer(60 * time.Second)
	defer timer.Stop()
	go func() {
		<-timer.C
		close(doneCh)
	}()
	// keep proposing for 60 seconds
	stopper := NewStopper()
	results := make([]uint64, 10000)
	for i := uint64(0); i < 10000; i++ {
		stopper.RunPWorker(func(arg interface{}) {
			workerID := arg.(uint64)
			groupID := (workerID % 48) + 1
			cmd := make([]byte, 16)
			results[workerID] = 0
			for {
				for j := 0; j < 32; j++ {
					rs, err := nh.Propose(groupID, cmd, 4*time.Second)
					if err != nil {
						panic(err)
					}
					v := <-rs.CompletedC
					if v.Completed() {
						results[workerID] = results[workerID] + 1
						rs.Release()
					}
				}
				select {
				case <-doneCh:
					return
				default:
				}
			}
		}, i)
	}
	stopper.Stop()
	total := uint64(0)
	for _, v := range results {
		total = total + v
	}
	fmt.Printf("total %d, %d proposals per second\n", total, total/60)
}
