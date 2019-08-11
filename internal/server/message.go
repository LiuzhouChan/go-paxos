package server

import (
	"sync"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

// MessageQueue is the queue used to hold Paxos messages.
type MessageQueue struct {
	size          uint64
	ch            chan struct{}
	left          []paxospb.PaxosMsg
	right         []paxospb.PaxosMsg
	snapshot      []paxospb.PaxosMsg
	leftInWrite   bool
	stopped       bool
	idx           uint64
	oldIdx        uint64
	cycle         uint64
	lazyFreeCycle uint64
	mu            sync.Mutex
}

// NewMessageQueue creates a new MessageQueue instance.
func NewMessageQueue(size uint64, ch bool, lazyFreeCycle uint64) *MessageQueue {
	q := &MessageQueue{
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]paxospb.PaxosMsg, size),
		right:         make([]paxospb.PaxosMsg, size),
		snapshot:      make([]paxospb.PaxosMsg, 0),
	}
	if ch {
		q.ch = make(chan struct{}, 1)
	}
	return q
}

// Close closes the queue so no further messages can be added.
func (q *MessageQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

// Notify notifies the notification channel listener that a new message is now
// available in the queue.
func (q *MessageQueue) Notify() {
	if q.ch != nil {
		select {
		case q.ch <- struct{}{}:
		default:
		}
	}
}

// Ch returns the notification channel.
func (q *MessageQueue) Ch() <-chan struct{} {
	return q.ch
}

func (q *MessageQueue) targetQueue() []paxospb.PaxosMsg {
	var t []paxospb.PaxosMsg
	if q.leftInWrite {
		t = q.left
	} else {
		t = q.right
	}
	return t
}

// Add adds the specified message to the queue.
func (q *MessageQueue) Add(msg paxospb.PaxosMsg) (bool, bool) {
	q.mu.Lock()
	if q.idx >= q.size {
		q.mu.Unlock()
		return false, q.stopped
	}
	if q.stopped {
		q.mu.Unlock()
		return false, true
	}
	w := q.targetQueue()
	w[q.idx] = msg
	q.idx++
	q.mu.Unlock()
	return true, false
}

//Clear the targetQueue
func (q *MessageQueue) gc() {
	if q.lazyFreeCycle > 0 {
		oldq := q.targetQueue()
		if q.lazyFreeCycle == 1 {
			for i := uint64(0); i < q.oldIdx; i++ {
				oldq[i].Entries = nil
			}
		} else if q.cycle%q.lazyFreeCycle == 0 {
			for i := uint64(0); i < q.size; i++ {
				oldq[i].Entries = nil
			}
		}
	}
}

// Get returns everything current in the queue.
func (q *MessageQueue) Get() []paxospb.PaxosMsg {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cycle++
	sz := q.idx
	q.idx = 0
	t := q.targetQueue()
	q.leftInWrite = !q.leftInWrite
	// clear the queue in the of side t
	q.gc()
	q.oldIdx = sz
	if len(q.snapshot) == 0 {
		return t[:sz]
	}
	ssm := q.snapshot
	q.snapshot = make([]paxospb.PaxosMsg, 0)
	return append(ssm, t[:sz]...)
}
