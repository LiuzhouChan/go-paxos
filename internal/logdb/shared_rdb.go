package logdb

import (
	"fmt"
	"path/filepath"

	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	"github.com/lni/dragonboat/raftio"

	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/internal/settings"
)

var (
	numOfRocksDBInstance = settings.Hard.LogDBPoolSize

	// RDBContextValueSize defines the size of byte array managed in RDB context.
	RDBContextValueSize uint64 = 1024 * 1024 * 64
)

// ShardedRDB is a LogDB implementation using sharded rocksdb instances.
type ShardedRDB struct {
	shards      []*RDB
	partitioner server.IPartitioner
}

// OpenShardedRDB ...
func OpenShardedRDB(dirs []string, lldirs []string) (*ShardedRDB, error) {
	shards := make([]*RDB, 0)
	for i := uint64(0); i < numOfRocksDBInstance; i++ {
		dir := filepath.Join(dirs[i], fmt.Sprintf("logdb-%d", i))
		lldir := ""
		if len(lldirs) > 0 {
			lldir = filepath.Join(lldirs[i], fmt.Sprintf("logdb-%d", i))
		}
		db, err := openRDB(dir, lldir)
		if err != nil {
			return nil, err
		}
		shards = append(shards, db)
	}
	partitioner := server.NewFixedPartitioner(numOfRocksDBInstance)
	mw := &ShardedRDB{
		shards:      shards,
		partitioner: partitioner,
	}
	return mw, nil
}

// Name returns the type name of the instance.
func (mw *ShardedRDB) Name() string {
	return LogDBType
}

// GetLogDBThreadContext return a IContext instance.
func (mw *ShardedRDB) GetLogDBThreadContext() paxosio.IContext {
	wb := mw.shards[0].getWriteBatch()
	return newRDBContext(RDBContextValueSize, wb)
}

// ListNodeInfo lists all available NodeInfo found in the log db.
func (mw *ShardedRDB) ListNodeInfo() ([]raftio.NodeInfo, error) {
	r := make([]paxosio.NodeInfo, 0)
	for _, v := range mw.shards {
		n, err := v.listNodeInfo()
		if err != nil {
			return nil, err
		}
		r = append(r, n...)
	}
	return r, nil
}

// SaveBootstrapInfo saves the specified bootstrap info for the given node.
func (mw *ShardedRDB) SaveBootstrapInfo(groupID uint64,
	nodeID uint64, bootstrap paxospb.Bootstrap) error {
	idx := mw.partitioner.GetPartitionID(groupID)
	return mw.shards[idx].saveBootstrapInfo(groupID, nodeID, bootstrap)
}

// GetBootstrapInfo returns the saved bootstrap info for the given node.
func (mw *ShardedRDB) GetBootstrapInfo(groupID uint64,
	nodeID uint64) (*paxospb.Bootstrap, error) {
	idx := mw.partitioner.GetPartitionID(groupID)
	return mw.shards[idx].getBootstrapInfo(groupID, nodeID)
}

//	IterateEntries ...
func (mw *ShardedRDB) IterateEntries(groupID uint64, nodeID uint64, low uint64,
	high uint64, maxSize uint64) ([]paxospb.Entry, error) {
	idx := mw.partitioner.GetPartitionID(groupID)
	return mw.shards[idx].iterateEntrys(groupID, nodeID, low, high)
}

// Close closes the ShardedRDB instance.
func (mw *ShardedRDB) Close() {
	for _, v := range mw.shards {
		v.close()
	}
}
