package paxos

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/paxospb"
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
	// ErrGroupClosed indicates that the requested cluster is being shut down.
	ErrGroupClosed = errors.New("paxos group already closed")
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
		err == ErrGroupClosed ||
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

type proposalShard struct {
	mu             sync.Mutex
	proposals      *entryQueue
	pending        map[uint64]*RequestState
	pool           *sync.Pool
	stopped        bool
	expireNotified uint64
	logicalClock
}

func newPendingProposalShard(pool *sync.Pool,
	proposals *entryQueue, tickInMilliSecond uint64) *proposalShard {
	gcTick := defaultGCTick
	if gcTick == 0 {
		panic("invalid gcTick")
	}
	lcu := logicalClock{
		tickInMillisecond: tickInMilliSecond,
		gcTick:            gcTick,
	}
	p := &proposalShard{
		proposals:    proposals,
		pending:      make(map[uint64]*RequestState),
		logicalClock: lcu,
		pool:         pool,
	}
	return p
}

func (p *proposalShard) propose(value []byte,
	key uint64, handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	timeoutTick := p.getTimeoutTick(timeout)
	if timeoutTick == 0 {
		return nil, ErrTimeoutTooSmall
	}
	if uint64(len(value)) > maxProposalPayloadSize {
		return nil, ErrPayloadTooBig
	}
	entry := paxospb.Entry{
		Key: key,
		AcceptorState: paxospb.AcceptorState{
			AccetpedValue: prepareProposalPayload(value),
		},
	}
	req := p.pool.Get().(*RequestState)
	req.completeHandler = handler
	req.key = entry.Key
	req.deadline = p.getTick() + timeoutTick
	if len(req.CompletedC) > 0 {
		req.CompletedC = make(chan RequestResult, 1)
	}
	p.mu.Lock()
	p.pending[entry.Key] = req
	p.mu.Unlock()
	added, stopped := p.proposals.add(entry)
	if stopped {
		plog.Warningf("dropping proposals, group stopped")
		p.mu.Lock()
		delete(p.pending, entry.Key)
		p.mu.Unlock()
		return nil, ErrGroupClosed
	}
	if !added {
		p.mu.Lock()
		delete(p.pending, entry.Key)
		p.mu.Unlock()
		plog.Warningf("dropping proposals, overloaded")
		return nil, ErrSystemBusy
	}
	return req, nil
}

func (p *proposalShard) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	if p.proposals != nil {
		p.proposals.close()
	}
	for _, c := range p.pending {
		c.notify(getTerminatedResult())
	}
}

func (p *proposalShard) getProposal(key, now uint64) *RequestState {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	ps, ok := p.pending[key]
	if ok && ps.deadline >= now {
		delete(p.pending, key)
		p.mu.Unlock()
		return ps
	}
	p.mu.Unlock()
	return nil
}

func (p *proposalShard) applied(key, result uint64, rejected bool) {
	now := p.getTick()
	var code RequestResultCode
	if rejected {
		code = requestRejected
	} else {
		code = requestCompleted
	}
	ps := p.getProposal(key, now)
	if ps != nil {
		ps.notify(RequestResult{code: code, result: result})
	}
	tick := p.getTick()
	if tick != p.expireNotified {
		p.gcAt(now)
		p.expireNotified = tick
	}
}

func (p *proposalShard) gc() {
	now := p.getTick()
	p.gcAt(now)
}

func (p *proposalShard) gcAt(now uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if now-p.lastGcTime < p.getTick() {
		return
	}
	p.lastGcTime = now
	deletedKeys := make(map[uint64]bool)
	for key, pRec := range p.pending {
		if pRec.deadline < now {
			pRec.notify(getTimeoutResult())
			deletedKeys[key] = true
		}
	}
	if len(deletedKeys) == 0 {
		return
	}
	for key := range deletedKeys {
		delete(p.pending, key)
	}
}

type keyGenerator struct {
	randMu sync.Mutex
	rand   *rand.Rand
}

func (k *keyGenerator) nextKey() uint64 {
	k.randMu.Lock()
	v := k.rand.Uint64()
	k.randMu.Unlock()
	return v
}

func getRandomGenerator(groupID, nodeID uint64,
	addr string, partition uint64) *keyGenerator {
	pid := os.Getpid()
	nano := time.Now().UnixNano()
	seedStr := fmt.Sprintf("%d-%d-%d-%d-%s-%d",
		pid, nano, groupID, nodeID, addr, partition)
	m := md5.New()
	if _, err := io.WriteString(m, seedStr); err != nil {
		panic(err)
	}
	md5sum := m.Sum(nil)
	seed := binary.LittleEndian.Uint64(md5sum)
	return &keyGenerator{rand: rand.New(rand.NewSource(int64(seed)))}
}

type pendingProposal struct {
	shards []*proposalShard
	keyg   []*keyGenerator
	ps     uint64
	idx    uint64
}

func newPendingProposal(pool *sync.Pool, proposals *entryQueue,
	groupID, nodeID uint64, paxosAddress string, tickInMilliSecond uint64) *pendingProposal {
	ps := uint64(16)
	p := &pendingProposal{
		shards: make([]*proposalShard, ps),
		keyg:   make([]*keyGenerator, ps),
		ps:     ps,
		idx:    0,
	}
	for i := uint64(0); i < ps; i++ {
		p.shards[i] = newPendingProposalShard(pool, proposals, tickInMilliSecond)
		p.keyg[i] = getRandomGenerator(groupID, nodeID, paxosAddress, i)
	}
	return p
}

func (p *pendingProposal) propose(value []byte, handler ICompleteHandler,
	timeout time.Duration) (*RequestState, error) {
	key := p.nextKey()
	pp := p.shards[key%p.ps]
	return pp.propose(value, key, handler, timeout)
}

func (p *pendingProposal) nextKey() uint64 {
	p.idx++
	return p.keyg[p.idx%p.ps].nextKey()
}

func (p *pendingProposal) close() {
	for _, pp := range p.shards {
		pp.close()
	}
}

func (p *pendingProposal) applied(key uint64, result uint64, rejected bool) {
	pp := p.shards[key%p.ps]
	pp.applied(key, result, rejected)
}

func (p *pendingProposal) increaseTick() {
	for i := uint64(0); i < p.ps; i++ {
		p.shards[i].increaseTick()
	}
}

func (p *pendingProposal) gc() {
	for i := uint64(0); i < p.ps; i++ {
		p.shards[i].gc()
	}
}

func prepareProposalPayload(cmd []byte) []byte {
	dst := make([]byte, len(cmd))
	copy(dst, cmd)
	return dst
}
