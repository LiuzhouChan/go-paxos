package rsm

import (
	"errors"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/logger"
)

var (
	plog = logger.GetLogger("rsm")
)

var (
	// ErrSaveSnapshot indicates there is error when trying to save a snapshot
	ErrSaveSnapshot = errors.New("failed to save snapshot")
	// ErrRestoreSnapshot indicates there is error when trying to restore
	// from a snapshot
	ErrRestoreSnapshot      = errors.New("failed to restore from snapshot")
	commitChanLength        = settings.Soft.NodeCommitChanLength
	commitChanBusyThreshold = settings.Soft.NodeCommitChanLength / 2
	batchedEntryApply       = settings.Soft.BatchedEntryApply > 0
)

// Commit is the processing units that can be handled by statemache
type Commit struct {
}
