package config

import (
	"github.com/LiuzhouChan/go-paxos/paxosio"
)

// PaxosRPCFactoryFunc is the factory function that creates the Raft RPC module
// instance for exchanging Raft messages between NodeHosts.
type PaxosRPCFactoryFunc func(NodeHostConfig,
	paxosio.RequestHandler) paxosio.IPaxosRPC

// LogDBFactoryFunc is the factory function that creates NodeHost's persistent
// storage module known as Log DB.
type LogDBFactoryFunc func(dirs []string,
	lowLatencyDirs []string) (paxosio.ILogDB, error)

//Config ...
type Config struct {
	NodeID     uint64
	GroupID    uint64
	IsFollower bool
	// SnapshotEntries defines how often state machine should be snapshotted
	// defined in terms of number of applied entries. Set SnapshotEntries to 0 to
	// disable Raft snapshotting and log compaction.
	SnapshotEntries uint64
}

// NodeHostConfig is the configuration used to configure NodeHost instances.
type NodeHostConfig struct {
	// WALDir is the directory used for storing the WAL of Paxos entries. It is
	// recommended to use low latency storage such as NVME SSD with power loss
	// protection to store such WAL data. Leave WALDir to have zero value will
	// have everything stored in NodeHostDir.
	WALDir string
	// NodeHostDir is where everything else is stored.
	NodeHostDir    string
	RTTMillisecond uint64
	// PaxosAddress is a hostname:port address used by the Paxos RPC module for
	// exchanging Paxos messages. This is also the identifier value to identify
	// a NodeHost instance.
	PaxosAddress string
	// LogDBFactory is the factory function used for creating the Log DB instance
	// used by NodeHost. The default zero value causes the default built-in RocksDB
	// based Log DB implementation to be used.
	LogDBFactory LogDBFactoryFunc
	// PaxosRPCFactory is the factory function used for creating the Paxos RPC
	// instance for exchanging Paxos message between NodeHost instances. The default
	// zero value causes the built-in TCP based RPC module to be used.
	PaxosRPCFactory PaxosRPCFactoryFunc
}
