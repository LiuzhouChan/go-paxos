package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
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
	exampleGroupID uint64 = 128
)

var (
	// initial nodes count is fixed to three, their addresses are also fixed
	addresses = []string{
		"localhost:63001",
		"localhost:63002",
		"localhost:63003",
	}
	errNotMembershipChange = errors.New("not a membership change request")
)

func main() {
	nodeID := flag.Int("nodeid", 1, "NodeID to use")
	addr := flag.String("addr", "", "Nodehost address")
	flag.Parse()
	// now we don't have join function, just set it to false
	join := false
	if len(*addr) == 0 && *nodeID != 1 && *nodeID != 2 && *nodeID != 3 {
		fmt.Fprintf(os.Stderr, "node id must be 1, 2 or 3 when address is not specified\n")
		os.Exit(1)
	}
	peers := make(map[uint64]string)
	if !join {
		for idx, v := range addresses {
			// key is the NodeID, NodeID is not allowed to be 0
			// value is the raft address
			peers[uint64(idx+1)] = v
		}
	}
	var nodeAddr string
	// for simplicity, in this example program, addresses of all those 3 initial
	// raft members are hard coded. when address is not specified on the command
	// line, we assume the node being launched is an initial raft member.
	if len(*addr) != 0 {
		nodeAddr = *addr
	} else {
		nodeAddr = peers[uint64(*nodeID)]
	}
	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)
	logger.GetLogger("paxos").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	rc := config.Config{
		NodeID:         uint64(*nodeID),
		GroupID:        exampleGroupID,
		AskForLearnRTT: 20,
	}
	datadir := filepath.Join(
		"example-data",
		"helloworld-data",
		fmt.Sprintf("node%d", *nodeID),
	)
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		PaxosAddress:   nodeAddr,
	}
	nh := paxos.NewNodeHost(nhc)
	if err := nh.StartGroup(peers, join, NewExampleStateMachine, rc); err != nil {
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
				nh.Stop()
				return
			}
			ch <- s
		}
	})
	paxosStopper.RunWorker(func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				result, err := nh.ReadLocalNode(exampleGroupID, []byte{})
				if err == nil {
					var count uint64
					count = binary.LittleEndian.Uint64(result)
					fmt.Fprintf(os.Stdout, "count: %d\n", count)
				}
			case <-paxosStopper.ShouldStop():
				return
			}
		}
	})
	paxosStopper.RunWorker(func() {
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				msg := strings.Replace(v, "\n", "", 1)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := nh.SyncPropose(ctx, exampleGroupID, []byte(msg))
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
