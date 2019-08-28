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

type entryQueue struct {
	size          uint64
	left          []paxospb.Entry
	right         []paxospb.Entry
	leftInWrite   bool
	stopped       bool
	idx           uint64
	oldIdx        uint64
	cycle         uint64
	lazyFreeCycle uint64
	mu            sync.Mutex
}

func newEntryQueue(size uint64, lazyFreeCycle uint64) *entryQueue {
	e := &entryQueue{
		size:          size,
		lazyFreeCycle: lazyFreeCycle,
		left:          make([]paxospb.Entry, size),
		right:         make([]paxospb.Entry, size),
	}
	return e
}

func (q *entryQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

func (q *entryQueue) targetQueue() []paxospb.Entry {
	var t []paxospb.Entry
	if q.leftInWrite {
		t = q.left
	} else {
		t = q.right
	}
	return t
}

func (q *entryQueue) add(ent paxospb.Entry) (bool, bool) {
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
	w[q.idx] = ent
	q.idx++
	q.mu.Unlock()
	return true, false
}

func (q *entryQueue) gc() {
	if q.lazyFreeCycle > 0 {
		// oldq := q.targetQueue()
		if q.lazyFreeCycle == 1 {
			for i := uint64(0); i < q.oldIdx; i++ {
				// oldq[i].Cmd = nil
			}
		} else if q.cycle%q.lazyFreeCycle == 0 {
			for i := uint64(0); i < q.size; i++ {
				// oldq[i].Cmd = nil
			}
		}
	}
}

func (q *entryQueue) get() []paxospb.Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cycle++
	sz := q.idx
	q.idx = 0
	t := q.targetQueue()
	q.leftInWrite = !q.leftInWrite
	q.gc()
	q.oldIdx = sz
	return t[:sz]
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
