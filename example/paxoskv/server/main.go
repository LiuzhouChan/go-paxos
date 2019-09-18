package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/LiuzhouChan/go-paxos"
	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/example/paxoskv"
	"github.com/LiuzhouChan/go-paxos/internal/utils/syncutil"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/sirupsen/logrus"
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
)

func main() {
	nodeID := flag.Int("nodeid", 1, "NodeID to use")
	grpc := flag.String("grpc", "localhost:10086", "the grpc port")
	flag.Parse()
	peers := make(map[uint64]string)
	for idx, v := range addresses {
		// key is the NodeID, NodeID is not allowed to be 0
		// value is the raft address
		peers[uint64(idx+1)] = v
	}
	nodeAddr := peers[uint64(*nodeID)]
	fmt.Fprintf(os.Stdout, "node address: %s\n", nodeAddr)
	logger.GetLogger("paxos").SetLevel(logger.WARNING)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	rc := config.Config{
		NodeID:         uint64(*nodeID),
		GroupID:        exampleGroupID,
		AskForLearnRTT: 20,
	}
	datadir := filepath.Join(
		"paxos-data",
		"kv-data",
		fmt.Sprintf("node%d", *nodeID),
	)
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		PaxosAddress:   nodeAddr,
	}
	nh := paxos.NewNodeHost(nhc)
	if err := nh.StartGroup(peers, false, paxoskv.NewDB, rc); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add group, %v\n", err)
		os.Exit(1)
	}
	grpcHost := *grpc
	paxoskvServer := paxoskv.NewPaxosKV(nh, grpcHost)
	paxoskvServer.Start()
	paxoskvStopper := syncutil.NewStopper()
	paxoskvStopper.RunWorker(func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		signal.Notify(c, syscall.SIGUSR2)
		for {
			select {
			case sig := <-c:
				if sig != syscall.SIGINT {
					continue
				}
				logrus.Infof("Ctrl+C pressed")
				paxoskvServer.Stop()
				nh.Stop()
				return
			case <-paxoskvStopper.ShouldStop():
				return
			}
		}
	})
	paxoskvStopper.Wait()
}
