package logdb

import "github.com/LiuzhouChan/go-paxos/logger"

var (
	plog = logger.GetLogger("logdb")
)

// RDB is the struct used to manage rocksdb backed persistent Log stores.
type RDB struct {
	keys *logdbKeyPool
	kvs  IKvStore
}
