package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/LiuzhouChan/go-paxos/paxospb"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/internal/utils/fileutil"
	"github.com/LiuzhouChan/go-paxos/internal/utils/random"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/paxosio"
)

var (
	plog = logger.GetLogger("server")
)

const (
	paxosAddressFilename = "paxos.address"
)

// Context is the server context for NodeHost.
type Context struct {
	hostname     string
	randomSource random.Source
	nhConfig     config.NodeHostConfig
	partitioner  IPartitioner
}

// NewContext creates and returns a new server Context object.
func NewContext(nhConfig config.NodeHostConfig) *Context {
	s := &Context{
		randomSource: random.NewLockedRand(),
		nhConfig:     nhConfig,
		partitioner:  NewFixedPartitioner(defaultClusterIDMod),
	}
	hostname, err := os.Hostname()
	if err != nil || len(hostname) == 0 {
		plog.Panicf("failed to get hostname %v", err)
	}
	s.hostname = hostname
	return s
}

// Stop stops the context.
func (sc *Context) Stop() {
}

// GetRandomSource returns the random source associated with the Nodehost.
func (sc *Context) GetRandomSource() random.Source {
	return sc.randomSource
}

// GetSnapshotDir returns the snapshot directory name.
func (sc *Context) GetSnapshotDir(groupID uint64,
	nodeID uint64) string {
	pd := fmt.Sprintf("snapshot-part-%d", sc.partitioner.GetPartitionID(groupID))
	sd := fmt.Sprintf("snapshot-%d-%d", groupID, nodeID)
	dirs := strings.Split(sc.nhConfig.NodeHostDir, ":")
	return filepath.Join(dirs[0], sc.hostname, pd, sd)
}

// GetLogDBDirs returns the directory names for LogDB
func (sc *Context) GetLogDBDirs() ([]string, []string) {
	lldirs := strings.Split(sc.nhConfig.WALDir, ":")
	dirs := strings.Split(sc.nhConfig.NodeHostDir, ":")
	// low latency dir not empty
	if len(sc.nhConfig.WALDir) > 0 {
		if len(lldirs) != len(dirs) {
			plog.Panicf("%d low latency dirs specified, but there are %d regular dirs",
				len(lldirs), len(dirs))
		}
	}
	for i := 0; i < len(dirs); i++ {
		dirs[i] = filepath.Join(dirs[i], sc.hostname)
	}
	if len(sc.nhConfig.WALDir) > 0 {
		for i := 0; i < len(dirs); i++ {
			lldirs[i] = filepath.Join(lldirs[i], sc.hostname)
		}
		return dirs, lldirs
	}
	return dirs, dirs
}

// CreateNodeHostDir creates the top level dirs used by nodehost.
func (sc *Context) CreateNodeHostDir() ([]string, []string) {
	nhDirs, walDirs := sc.GetLogDBDirs()
	exists := func(path string) (bool, error) {
		_, err := os.Stat(path)
		if err == nil {
			return true, nil
		}
		if os.IsNotExist(err) {
			return false, nil
		}
		return true, err
	}
	for i := 0; i < len(nhDirs); i++ {
		walExist, err := exists(walDirs[i])
		if err != nil {
			panic(err)
		}
		nhExist, err := exists(nhDirs[i])
		if err != nil {
			panic(err)
		}
		if !walExist {
			if err := fileutil.MkdirAll(walDirs[i]); err != nil {
				panic(err)
			}
		}
		if !nhExist {
			if err := fileutil.MkdirAll(nhDirs[i]); err != nil {
				panic(err)
			}
		}
	}
	return nhDirs, walDirs
}

// PrepareSnapshotDir creates the snapshot directory for the specified node.
func (sc *Context) PrepareSnapshotDir(groupID uint64, nodeID uint64) (string, error) {
	snapshotDir := sc.GetSnapshotDir(groupID, nodeID)
	if err := fileutil.MkdirAll(snapshotDir); err != nil {
		return "", err
	}
	return snapshotDir, nil
}

// CheckNodeHostDir checks whether NodeHost dir is owned by the
// current nodehost.
func (sc *Context) CheckNodeHostDir(addr string) {
	dirs, lldirs := sc.GetLogDBDirs()
	for i := 0; i < len(dirs); i++ {
		sc.checkDirAddressMatch(dirs[i], addr)
		sc.checkDirAddressMatch(lldirs[i], addr)
	}
}

func (sc *Context) checkDirAddressMatch(dir string, addr string) {
	fp := filepath.Join(dir, paxosAddressFilename)
	se := func(s1 string, s2 string) bool {
		return strings.ToLower(strings.TrimSpace(s1)) ==
			strings.ToLower(strings.TrimSpace(s2))
	}
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		status := paxospb.PaxosDataStatus{
			Address:  addr,
			BinVer:   paxosio.LogDBBinVersion,
			HardHash: settings.Hard.Hash(),
		}
		err = fileutil.CreateFlagFile(dir, paxosAddressFilename, &status)
		if err != nil {
			panic(err)
		}
	} else {
		status := paxospb.PaxosDataStatus{}
		err := fileutil.GetFlagFileContent(dir, paxosAddressFilename, &status)
		if err != nil {
			panic(err)
		}
		if !se(string(status.Address), addr) {
			plog.Panicf("nodehost data dirs belong to different nodehost %s",
				strings.TrimSpace(status.Address))
		}
		if status.BinVer != paxosio.LogDBBinVersion {
			plog.Panicf("binary compatibility version, data dir %d, software %d",
				status.BinVer, paxosio.LogDBBinVersion)
		}
		if status.HardHash != settings.Hard.Hash() {
			plog.Panicf("had hash mismatch, hard settings changed?")
		}
	}
}
