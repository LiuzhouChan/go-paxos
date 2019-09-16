package paxos

import (
	"bytes"
	"reflect"
	"sync"
	"testing"
	"time"
)

const (
	testTickInMillisecond uint64 = 50
)

func TestRequestStateRelease(t *testing.T) {
	rs := RequestState{
		data:     make([]byte, 10),
		key:      100,
		deadline: 500,
		pool:     &sync.Pool{},
	}
	exp := RequestState{pool: rs.pool}
	rs.Release()
	if !reflect.DeepEqual(&exp, &rs) {
		t.Errorf("unexpected state, got %+v, want %+v", rs, exp)
	}
}

func getPendingProposal() (*pendingProposal, *entryQueue) {
	c := newEntryQueue(5)
	p := &sync.Pool{}
	p.New = func() interface{} {
		obj := &RequestState{}
		obj.pool = p
		obj.CompletedC = make(chan RequestResult, 1)
		return obj
	}
	return newPendingProposal(p, c, 100, 120, "nodehost:12345", testTickInMillisecond), c
}

func TestPendingProposalCanBeCreatedAndClosed(t *testing.T) {
	pp, c := getPendingProposal()
	if _, ok := c.get(); ok {
		t.Errorf("unexpected item in entry queue")
	}
	pp.close()
	if !c.stopped {
		t.Errorf("entry queue not closed")
	}
}

func countPendingProposal(p *pendingProposal) int {
	total := 0
	for i := uint64(0); i < p.ps; i++ {
		total += len(p.shards[i].pending)
	}
	return total
}

func TestProposalCanBeProposed(t *testing.T) {
	pp, c := getPendingProposal()
	rs, err := pp.propose([]byte("test data"), nil, time.Second)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	if countPendingProposal(pp) != 1 {
		t.Errorf("len(pending)=%d, want 1", countPendingProposal(pp))
	}
	select {
	case <-rs.CompletedC:
		t.Errorf("not suppose to have anything completed")
	default:
	}
	_, ok := c.get()
	if !ok {
		t.Errorf("cannot get a propose from pending proposal")
	}
	pp.close()
	select {
	case v := <-rs.CompletedC:
		if !v.Terminated() {
			t.Errorf("get %d, want %d", v, requestTerminated)
		}
	default:
		t.Errorf("suppose to return terminated")
	}
}

func TestProposeOnClosedPendingProposalReturnError(t *testing.T) {
	pp, _ := getPendingProposal()
	pp.close()
	_, err := pp.propose([]byte("test data"), nil, time.Second)
	if err != ErrGroupClosed {
		t.Errorf("unexpected err %v", err)
	}
}

func TestProposalCanBeCompleted(t *testing.T) {
	pp, _ := getPendingProposal()
	rs, err := pp.propose([]byte("test data"), nil, time.Second)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	pp.applied(rs.key+1, 0, false)
	select {
	case <-rs.CompletedC:
		t.Errorf("unexpected applied proposal with invalid key")
	default:
	}
	if countPendingProposal(pp) == 0 {
		t.Errorf("pending is empty")
	}
	pp.applied(rs.key, 0, false)
	select {
	case v := <-rs.CompletedC:
		if !v.Completed() {
			t.Errorf("get %d, want %d", v, requestCompleted)
		}
	default:
		t.Errorf("expect to get complete signal")
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending is not empty")
	}
}

func TestProposalCanBeExpired(t *testing.T) {
	pp, _ := getPendingProposal()
	timeout := time.Duration(1000 * time.Millisecond)
	rs, err := pp.propose([]byte("test data"), nil, timeout)
	if err != nil {
		t.Errorf("failed to make proposal, %v", err)
	}
	tickCount := uint64(1000 / testTickInMillisecond)
	for i := uint64(0); i < tickCount; i++ {
		pp.increaseTick()
		pp.gc()
	}
	plog.Infof("%v", pp.shards[0].getTick())
	select {
	case <-rs.CompletedC:
		t.Errorf("not suppose to return anything")
	default:
	}
	for i := uint64(0); i < defaultGCTick+1; i++ {
		pp.increaseTick()
		pp.gc()
	}
	select {
	case v := <-rs.CompletedC:
		if !v.Timeout() {
			t.Errorf("got %d, want %d", v, requestTimeout)
		}
	default:
	}
	if countPendingProposal(pp) != 0 {
		t.Errorf("pending/keys is not empty")
	}
}

func TestProposalErrorsAreReported(t *testing.T) {
	pp, _ := getPendingProposal()
	for i := 0; i < 5; i++ {
		_, err := pp.propose([]byte("test data"), nil, time.Second)
		if err != nil {
			t.Errorf("propose failed")
		}
	}
	_, err := pp.propose([]byte("test data"), nil, time.Second)
	if err != ErrSystemBusy {
		t.Errorf("suppose to return ErrSystemBusy")
	}
	pp, _ = getPendingProposal()
	var buffer bytes.Buffer
	for i := uint64(0); i < maxProposalPayloadSize; i++ {
		buffer.WriteString("a")
	}
	data := buffer.Bytes()
	_, err = pp.propose(data, nil, time.Second)
	if err != nil {
		t.Errorf("suppose to be successful")
	}
	buffer.WriteString("a")
	data = buffer.Bytes()
	_, err = pp.propose(data, nil, time.Second)
	if err != ErrPayloadTooBig {
		t.Errorf("suppose to return ErrPayloadTooBig")
	}
}

func TestClosePendingProposalIgnoresStepEngineActivities(t *testing.T) {
	pp, _ := getPendingProposal()
	rs, _ := pp.propose(nil, nil, time.Duration(5*time.Second))
	select {
	case <-rs.CompletedC:
		t.Fatalf("completedC is already signalled")
	default:
	}
	for i := uint64(0); i < pp.ps; i++ {
		pp.shards[i].stopped = true
	}
	pp.applied(rs.key, 1, false)
	select {
	case <-rs.CompletedC:
		t.Fatalf("completeC unexpectedly signaled")
	default:
	}
}
