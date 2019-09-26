// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paxos

import (
	"sync"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

type queueBuffer struct {
	buffer      []interface{}
	readerIndex uint64
	writerIndex uint64
}

func newQueueBuffer(size uint64) *queueBuffer {
	return &queueBuffer{
		buffer:      make([]interface{}, size),
		readerIndex: 0,
		writerIndex: 0,
	}
}

func (q *queueBuffer) readableSlices() uint64 {
	return q.writerIndex - q.readerIndex
}

func (q *queueBuffer) writableSlices() uint64 {
	return uint64(len(q.buffer)) - q.writerIndex
}

func (q *queueBuffer) prependableSlices() uint64 {
	return q.readerIndex
}

func (q *queueBuffer) peek() interface{} {
	if q.readableSlices() <= 0 {
		panic("peek when readableSlice <= 0")
	}
	ret := q.buffer[q.readerIndex]
	q.readerIndex++
	return ret
}

func (q *queueBuffer) append(data interface{}) {
	q.ensureWritableBytes()
	q.buffer[q.writerIndex] = data
	q.writerIndex++
}

func (q *queueBuffer) ensureWritableBytes() {
	if q.writableSlices() > 0 {
		return
	}
	//  here the writableSlice is 0, and the prependableSlice is 0
	if q.prependableSlices() <= 0 {
		// resize the buffer
		buffer := make([]interface{}, 2*len(q.buffer))
		copy(buffer, q.buffer)
		q.buffer = buffer
	} else {
		// move the readable data to the front
		readable := q.readableSlices()
		copy(q.buffer[0:], q.buffer[q.readerIndex:q.writerIndex])
		q.readerIndex = 0
		q.writerIndex = q.readerIndex + readable
		// n := uint64(len(q.buffer))
		// for i := q.writerIndex; i < n; i++ {
		// 	q.buffer[i] = nil
		// }
	}
}

type qnode struct {
	val  interface{}
	next *qnode
}

type queue struct {
	head *qnode
	tail *qnode
	size uint64
	pool *sync.Pool
}

func newQueue() *queue {
	return &queue{
		head: nil,
		tail: nil,
		size: 0,
		pool: &sync.Pool{
			New: func() interface{} {
				return &qnode{
					next: nil,
				}
			},
		},
	}
}

func (q *queue) empty() bool {
	return q.size == 0
}

func (q *queue) enqueue(val interface{}) {
	node := q.pool.Get().(*qnode)
	node.val = val
	if q.size == 0 {
		q.head = node
		q.tail = node
	} else {
		q.tail.next = node
		q.tail = q.tail.next
	}
	q.size++
}

func (q *queue) dequeue() interface{} {
	if q.size == 0 {
		panic("dequeue from an empty queue")
	}
	node := q.head
	val := node.val
	if q.size == 1 {
		q.head = nil
		q.tail = nil
	} else {
		q.head = q.head.next
	}
	q.size--
	q.pool.Put(node)
	return val
}

type entryQueue struct {
	size uint64
	q    *queue
	// q       *queueBuffer
	stopped bool
	mu      sync.Mutex
}

func newEntryQueue(size uint64) *entryQueue {
	e := &entryQueue{
		size: size,
		q:    newQueue(),
		// q: newQueueBuffer(size * 2),
	}
	return e
}

func (q *entryQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

func (q *entryQueue) add(ent paxospb.Entry) (bool, bool) {
	q.mu.Lock()
	if q.q.size >= q.size {
		q.mu.Unlock()
		return false, q.stopped
	}
	if q.stopped {
		q.mu.Unlock()
		return false, true
	}
	q.q.enqueue(ent)
	q.mu.Unlock()
	return true, false
}

func (q *entryQueue) get() (paxospb.Entry, bool) {
	q.mu.Lock()
	if q.stopped || q.q.empty() {
		q.mu.Unlock()
		return paxospb.Entry{}, false
	}
	ent := q.q.dequeue()
	q.mu.Unlock()
	return ent.(paxospb.Entry), true
}

type readyGroup struct {
	mu    sync.Mutex
	ready map[uint64]struct{}
	maps  [2]map[uint64]struct{}
	index uint8
}

func newReadyGroup() *readyGroup {
	r := &readyGroup{}
	r.maps[0] = make(map[uint64]struct{})
	r.maps[1] = make(map[uint64]struct{})
	r.ready = r.maps[0]
	return r
}

func (r *readyGroup) setGroupReady(groupID uint64) {
	r.mu.Lock()
	r.ready[groupID] = struct{}{}
	r.mu.Unlock()
}

func (r *readyGroup) getReadyGroups() map[uint64]struct{} {
	r.mu.Lock()
	v := r.ready
	r.index++
	selected := r.index % 2
	nm := r.maps[selected]
	for k := range nm {
		delete(nm, k)
	}
	r.ready = nm
	r.mu.Unlock()
	return v
}
