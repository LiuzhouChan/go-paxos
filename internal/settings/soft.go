package settings

// Soft is the soft settings that can be changed after the deployment of a
// system.
var Soft = getSoftSettings()

type soft struct {
	StreamConnections           uint64
	LazyFreeCycle               uint64
	IncomingProposalQueueLength uint64

	// NodeCommitChanLength defined the length of each node's commitC channel.
	NodeCommitChanLength uint64

	SendQueueLength uint64

	PerConnectionBufferSize uint64
	// PerCpnnectionRecvBufSize is the size of the recv buffer size.
	PerConnectionRecvBufSize  uint64
	GetConnectedTimeoutSecond uint64

	BatchedEntryApply uint64

	StepEngineCommitWorkerCount uint64
	NodeReloadMillisecond       uint64

	// UseRangeDelete determines whether to use range delete when possible.
	UseRangeDelete bool
	// RDBMaxBackgroundCompactions is the MaxBackgroundCompactions parameter
	// directly passed to rocksdb.
	RDBMaxBackgroundCompactions uint64
	// RDBMaxBackgroundFlushes is the MaxBackgroundFlushes parameter directly
	// passed to rocksdb.
	RDBMaxBackgroundFlushes uint64
	// RDBLRUCacheSize is the LRUCacheSize
	RDBLRUCacheSize uint64

	LatencySampleRatio uint64

	NodeHostSyncPoolSize uint64
}

func getSoftSettings() soft {
	org := getDefaultSoftSettings()
	return org
}

func getDefaultSoftSettings() soft {
	return soft{
		StreamConnections:           4,
		LazyFreeCycle:               1,
		NodeCommitChanLength:        1024,
		SendQueueLength:             1024 * 8,
		PerConnectionBufferSize:     64 * 1024 * 1024,
		PerConnectionRecvBufSize:    64 * 1024,
		GetConnectedTimeoutSecond:   5,
		NodeReloadMillisecond:       200,
		IncomingProposalQueueLength: 2048,
		StepEngineCommitWorkerCount: 16,
		RDBMaxBackgroundCompactions: 2,
		RDBMaxBackgroundFlushes:     2,
		RDBLRUCacheSize:             0,
		BatchedEntryApply:           0,
		NodeHostSyncPoolSize:        8,
		LatencySampleRatio:          0,
	}
}
