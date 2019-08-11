package settings

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/LiuzhouChan/go-paxos/logger"
)

var (
	plog = logger.GetLogger("settings")
)

// Hard is the hard settings that can not be changed after the system has been
// deployed.
var Hard = getHardSettings()

type hard struct {
	// StepEngineWorkerCount defines number of workers to use to step raft node
	// changes, typically each in its own goroutine. This turn determines how
	// nodes are partitioned to different entry cache and rdb instances.
	StepEngineWorkerCount uint64
	// LogDBPoolSize defines how many rdb instances to use. We use multiple rdb
	// instance so different disks can be used together to get better combined IO
	// performance.
	LogDBPoolSize uint64
	// LRUMaxSessionCount is the max number of client sessions that can be
	// concurrently held and managed by each raft cluster.
	LRUMaxSessionCount uint64
	// LogDBEntryBatchSize is the max size of each entry batch.
	LogDBEntryBatchSize uint64
}

const (
	//
	// RSM
	//

	// SnapshotHeaderSize defines the snapshot header size in number of bytes.
	SnapshotHeaderSize uint64 = 1024

	//
	// transport
	//

	// MaxProposalPayloadSize is the max size allowed for a proposal payload.
	MaxProposalPayloadSize uint64 = 32 * 1024 * 1024
	// MaxMessageSize is the max size for a single gRPC message sent between
	// raft nodes. It must be greater than MaxProposalPayloadSize and smaller
	// than the current default of max gRPC send/receive size (4MBytes).
	MaxMessageSize uint64 = 2*MaxProposalPayloadSize + 2*1024*1024
	// SnapshotChunkSize is the snapshot chunk size sent by the gRPC transport
	// module.
	SnapshotChunkSize uint64 = 2 * 1024 * 1024

	//
	// Drummer DB
	//

	// LaunchDeadlineTick defines the number of ticks allowed for the bootstrap
	// process to complete.
	LaunchDeadlineTick uint64 = 24
)

func (h *hard) Hash() uint64 {
	hashstr := fmt.Sprintf("%d-%d-%t-%d-%d",
		h.StepEngineWorkerCount,
		h.LogDBPoolSize,
		false, // was the UseRangeDelete option
		h.LRUMaxSessionCount,
		h.LogDBEntryBatchSize)
	mh := md5.New()
	if _, err := io.WriteString(mh, hashstr); err != nil {
		panic(err)
	}
	md5hash := mh.Sum(nil)
	return binary.LittleEndian.Uint64(md5hash)
}

func getHardSettings() hard {
	org := getDefaultHardSettings()
	return org
}

func getDefaultHardSettings() hard {
	return hard{
		StepEngineWorkerCount: 16,
		LogDBPoolSize:         16,
		LRUMaxSessionCount:    4096,
		LogDBEntryBatchSize:   48,
	}
}
