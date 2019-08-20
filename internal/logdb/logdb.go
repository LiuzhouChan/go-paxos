package logdb

import (
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/paxosio"
)

var (
	plog = logger.GetLogger("logdb")
)

func checkDirs(dirs []string, lldirs []string) {
	if len(dirs) == 1 {
		if len(lldirs) != 0 && len(lldirs) != 1 {
			plog.Panicf("only 1 regular dir but %d low latency dirs", len(lldirs))
		}
	} else if len(dirs) > 1 {
		if uint64(len(dirs)) != numOfRocksDBInstance {
			plog.Panicf("%d regular dirs, but expect to have %d rdb instances",
				len(dirs), numOfRocksDBInstance)
		}
		if len(lldirs) > 0 {
			if len(dirs) != len(lldirs) {
				plog.Panicf("%v regular dirs, but %v low latency dirs", dirs, lldirs)
			}
		}
	} else {
		panic("no regular dir")
	}
}

// OpenLogDB opens a LogDB instance using the default implementation.
func OpenLogDB(dirs []string, lowLatencyDirs []string) (paxosio.ILogDB, error) {
	checkDirs(dirs, lowLatencyDirs)
	llDirRequired := len(lowLatencyDirs) == 1
	if len(dirs) == 1 {
		for i := uint64(1); i < numOfRocksDBInstance; i++ {
			dirs = append(dirs, dirs[0])
			if llDirRequired {
				lowLatencyDirs = append(lowLatencyDirs, lowLatencyDirs[0])
			}
		}
	}
	return OpenShardedRDB(dirs, lowLatencyDirs)
}
