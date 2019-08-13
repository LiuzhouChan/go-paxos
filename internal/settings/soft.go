package settings

// Soft is the soft settings that can be changed after the deployment of a
// system.
var Soft = getSoftSettings()

type soft struct {
	StreamConnections uint64
	LazyFreeCycle     uint64

	// NodeCommitChanLength defined the length of each node's commitC channel.
	NodeCommitChanLength uint64

	SendQueueLength uint64

	PerConnectionBufferSize uint64
	// PerCpnnectionRecvBufSize is the size of the recv buffer size.
	PerConnectionRecvBufSize  uint64
	GetConnectedTimeoutSecond uint64

	BatchedEntryApply uint64
}

func getSoftSettings() soft {
	org := getDefaultSoftSettings()
	return org
}

func getDefaultSoftSettings() soft {
	return soft{
		StreamConnections:         4,
		LazyFreeCycle:             1,
		NodeCommitChanLength:      1024,
		SendQueueLength:           1024 * 8,
		PerConnectionBufferSize:   64 * 1024 * 1024,
		PerConnectionRecvBufSize:  64 * 1024,
		GetConnectedTimeoutSecond: 5,
		BatchedEntryApply:         0,
	}
}
