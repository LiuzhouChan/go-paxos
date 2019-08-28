package paxos

import (
	"sync"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/rsm"
	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

type node struct {
	paxosAddress      string
	config            config.Config
	commitC           chan<- rsm.Commit
	mq                *server.MessageQueue
	lastApplied       uint64
	commitReady       func(uint64)
	sendPaxosMessage  func(paxospb.PaxosMsg)
	sm                *rsm.StateMachine
	incomingProposals *entryQueue
	paxosMu           sync.Mutex
}
