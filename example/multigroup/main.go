package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LiuzhouChan/go-paxos"
	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/lni/dragonboat-example/utils"
)

const (
	// we use two paxos groups in this example, they are identified by the group
	// ID values below
	groupID1 uint64 = 100
	groupID2 uint64 = 101
)

var (
	// initial nodes count is three, their addresses are also fixed
	// this is for simplicity
	addresses = []string{
		"localhost:63001",
		"localhost:63002",
		"localhost:63003",
	}
)

func main() {
	nodeID := flag.Int("nodeid", 1, "NodeID to use")
	flag.Parse()
	if *nodeID > 3 || *nodeID < 1 {
		fmt.Fprintf(os.Stderr, "invalid nodeid %d, it must be 1, 2 or 3", *nodeID)
		os.Exit(1)
	}
	peers := make(map[uint64]string)
	for idx, v := range addresses {
		// key is the NodeID, NodeID is not allowed to be 0
		// value is the raft address
		peers[uint64(idx+1)] = v
	}
	nodeAddr := peers[uint64(*nodeID)]
	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)
	// change the log verbosity
	logger.GetLogger("paxos").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)

	rc := config.Config{
		NodeID:         uint64(*nodeID),
		AskForLearnRTT: 20,
	}
	datadir := filepath.Join(
		"example-data",
		"multigroup-data",
		fmt.Sprintf("node%d", *nodeID))

	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		PaxosAddress:   nodeAddr,
	}

	nh := paxos.NewNodeHost(nhc)
	defer nh.Stop()

	// start the first group
	// we use ExampleStateMachine as the IStateMachine for this group, its
	// behaviour is identical to the one used in the Hello World example.
	rc.GroupID = groupID1
	if err := nh.StartGroup(peers, false, NewExampleStateMachine, rc); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add group, %v\n", err)
		os.Exit(1)
	}
	// start the second group
	// we use SecondStateMachine as the IStateMachine for the second group
	rc.GroupID = groupID2
	if err := nh.StartGroup(peers, false, NewSecondStateMachine, rc); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add group, %v\n", err)
		os.Exit(1)
	}

	paxosStopper := utils.NewStopper()
	consoleStopper := utils.NewStopper()
	ch := make(chan string, 16)
	consoleStopper.RunWorker(func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				close(ch)
				return
			}
			if s == "exit\n" {
				paxosStopper.Stop()
				// no data will be lost/corrupted if nodehost.Stop() is not called
				nh.Stop()
				return
			}
			ch <- s
		}
	})

	paxosStopper.RunWorker(func() {
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				// remove the \n char
				msg := strings.Replace(strings.TrimSpace(v), "\n", "", 1)
				var err error
				// In this example, the strategy on how data is sharded across different
				// Raft groups is based on whether the input message ends with a "?".
				// In your application, you are free to choose strategies suitable for
				// your application.
				if strings.HasSuffix(msg, "?") {
					// user message ends with "?", make a proposal to update the second
					// raft group
					_, err = nh.SyncPropose(ctx, groupID1, []byte(msg))
				} else {
					// message not ends with "?", make a proposal to update the first
					// raft group
					_, err = nh.SyncPropose(ctx, groupID2, []byte(msg))
				}
				cancel()
				if err != nil {
					fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
				}
			case <-paxosStopper.ShouldStop():
				return
			}
		}
	})
	paxosStopper.Wait()
}
