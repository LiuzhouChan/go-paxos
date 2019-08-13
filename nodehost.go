package paxos

import "sync"

const (
	PaxosMajor = 0
	PaxosMinor = 0
	PaxosPatch = 1
	// DEVVersion is a boolean value to indicate whether this is a dev version
	DEVVersion = true
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
