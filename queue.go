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
	lane "gopkg.in/oleiade/lane.v1"
)

type entryQueue struct {
	size    int
	queue   *lane.Queue
	stopped bool
	mu      sync.Mutex
}

func newEntryQueue(size int) *entryQueue {
	e := &entryQueue{
		size:  size,
		queue: lane.NewQueue(),
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
	if q.queue.Size() >= q.size {
		q.mu.Unlock()
		return false, q.stopped
	}
	if q.stopped {
		q.mu.Unlock()
		return false, true
	}
	q.queue.Enqueue(ent)
	q.mu.Unlock()
	return true, false
}

func (q *entryQueue) get() (paxospb.Entry, bool) {
	q.mu.Lock()
	if q.stopped || q.queue.Empty() {
		q.mu.Unlock()
		return paxospb.Entry{}, false
	}
	ent := q.queue.Dequeue()
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
