package paxos

import (
	"sync"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
)

const (
	//PaxosMajor ...
	PaxosMajor = 0
	//PaxosMinor ...
	PaxosMinor = 0
	//PaxosPatch ...
	PaxosPatch = 1
	// DEVVersion is a boolean value to indicate whether this is a dev version
	DEVVersion = true
)

var (
	delaySampleRatio uint64 = settings.Soft.LatencySampleRatio
)

//NodeHost ...
type NodeHost struct {
	tick     uint64
	msgCount uint64
	groupMu  struct {
		sync.RWMutex
		stopped bool
		gsi     uint64
		groups  sync.Map
	}
	transportLatency *sample
}
