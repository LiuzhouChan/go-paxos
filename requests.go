package paxos

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/logger"
)

const (
	badKeyCheck      bool   = false
	sysGcMillisecond uint64 = 15000
)

var (
	defaultGCTick uint64 = 2
	plog                 = logger.GetLogger("go-paxos")
)

var (
	// ErrInvalidSession indicates that the specified client session is invalid.
	ErrInvalidSession = errors.New("invalid session")
	// ErrTimeoutTooSmall indicates that the specified timeout value is too small.
	ErrTimeoutTooSmall = errors.New("specified timeout value is too small")
	// ErrPayloadTooBig indicates that the payload is too big.
	ErrPayloadTooBig = errors.New("payload is too big")
	// ErrSystemBusy indicates that the system is too busy to handle the request.
	ErrSystemBusy = errors.New("system is too busy try again later")
	// ErrClusterClosed indicates that the requested cluster is being shut down.
	ErrClusterClosed = errors.New("raft cluster already closed")
	// ErrBadKey indicates that the key is bad, retry the request is recommended.
	ErrBadKey = errors.New("bad key try again later")
	// ErrPendingConfigChangeExist indicates that there is already a pending
	// membership change exist in the system.
	ErrPendingConfigChangeExist = errors.New("pending config change request exist")
	// ErrTimeout indicates that the operation timed out.
	ErrTimeout = errors.New("timeout")
	// ErrSystemStopped indicates that the system is being shut down.
	ErrSystemStopped = errors.New("system stopped")
	// ErrCanceled indicates that the request has been canceled.
	ErrCanceled = errors.New("request canceled")
	// ErrRejected indicates that the request has been rejected.
	ErrRejected = errors.New("request rejected")
)

// IsTempError returns a boolean value indicating whether the specified error
// is a temporary error that worth to be retried later with the exact same
// input, potentially on a more suitable NodeHost instance.
func IsTempError(err error) bool {
	return err == ErrSystemBusy ||
		err == ErrBadKey ||
		err == ErrPendingConfigChangeExist ||
		err == ErrClusterClosed ||
		err == ErrSystemStopped
}

// RequestResultCode is the result code returned to the client to indicate the
// outcome of the request.
type RequestResultCode int

// RequestResult is the result struct returned for the request.
type RequestResult struct {
	// code is the result state of the request.
	code RequestResultCode
	// Result is the returned result from the Update method of the IStateMachine
	// instance. Result is only available when making a proposal and the Code
	// value is RequestCompleted.
	result uint64
}

// Timeout returns a boolean value indicating whether the Request timed out.
func (rr *RequestResult) Timeout() bool {
	return rr.code == requestTimeout
}

// Completed returns a boolean value indicating the request request completed
// successfully. For proposals, it means the proposal has been committed by the
// Raft cluster and applied on the local node. For ReadIndex operation, it means
// the cluster is now ready for a local read.
func (rr *RequestResult) Completed() bool {
	return rr.code == requestCompleted
}

// Terminated returns a boolean value indicating the request terminated due to
// the requested Raft cluster is being shut down.
func (rr *RequestResult) Terminated() bool {
	return rr.code == requestTerminated
}

// Rejected returns a boolean value indicating the request is rejected. For a
// proposal, it means that the used client session instance is not registered
// or it has been evicted on the server side. When requesting a client session
// to be registered, Rejected means the another client session with the same
// client ID has already been registered. When requesting a client session to
// be unregistered, Rejected means the specified client session is not found
// on the server side. For a membership change request, it means the request
// is out of order and thus ignored. Note that the out-of-order check when
// making membership changes is only imposed when IMasterClient is used in
// NodeHost.
func (rr *RequestResult) Rejected() bool {
	return rr.code == requestRejected
}

// GetResult returns the result value of the request. When making a proposal,
// the returned result is the value returned by the Update method of the
// IStateMachine instance.
func (rr *RequestResult) GetResult() uint64 {
	return rr.result
}

const (
	requestTimeout RequestResultCode = iota
	requestCompleted
	requestTerminated
	requestRejected
)

var requestResultCodeName = [...]string{
	"RequestTimeout",
	"RequestCompleted",
	"RequestTerminated",
	"RequestRejected",
}

func (c RequestResultCode) String() string {
	return requestResultCodeName[uint64(c)]
}

func getTerminatedResult() RequestResult {
	return RequestResult{
		code: requestTerminated,
	}
}

func getTimeoutResult() RequestResult {
	return RequestResult{
		code: requestTimeout,
	}
}

const (
	maxProposalPayloadSize = settings.MaxProposalPayloadSize
)

type logicalClock struct {
	tick              uint64
	lastGcTime        uint64
	gcTick            uint64
	tickInMillisecond uint64
}

func (p *logicalClock) increaseTick() {
	atomic.AddUint64(&p.tick, 1)
}

func (p *logicalClock) getTick() uint64 {
	return atomic.LoadUint64(&p.tick)
}

func (p *logicalClock) getTimeoutTick(timeout time.Duration) uint64 {
	timeoutMs := uint64(timeout.Nanoseconds() / 1000000)
	return timeoutMs / p.tickInMillisecond
}

// ICompleteHandler is a handler interface that will be invoked when the request
// in completed. This interface is used by the language bindings, applications
// are not expected to directly use this interface.
type ICompleteHandler interface {
	Notify(RequestResult)
	Release()
}

// RequestState is the object used to provide request result to users.
type RequestState struct {
	data            []byte
	key             uint64
	deadline        uint64
	completeHandler ICompleteHandler
	// CompleteC is a channel for delivering request result to users.
	CompletedC chan RequestResult
	node       *node
	pool       *sync.Pool
}

func (r *RequestState) notify(result RequestResult) {
	if r.completeHandler == nil {
		select {
		case r.CompletedC <- result:
		default:
			plog.Panicf("RequestState.CompletedC is full")
		}
	} else {
		r.completeHandler.Notify(result)
		r.completeHandler.Release()
		r.Release()
	}
}

// Release puts the RequestState object back to the sync.Pool pool.
func (r *RequestState) Release() {
	if r.pool != nil {
		r.data = nil
		r.key = 0
		r.completeHandler = nil
		r.node = nil
		r.pool.Put(r)
	}
}
