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
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

func TestEntryQueueCanBeCreated(t *testing.T) {
	q := newEntryQueue(5, 0)
	if q.size != 5 || len(q.left) != 5 || len(q.right) != 5 {
		t.Errorf("size unexpected")
	}
	if q.idx != 0 {
		t.Errorf("idx %d, want 0", q.idx)
	}
}

func TestLazyFreeCanBeDisabled(t *testing.T) {
	q := newEntryQueue(5, 0)
	q.add(paxospb.Entry{Cmd: make([]byte, 16)})
	q.add(paxospb.Entry{Cmd: make([]byte, 16)})
	q.add(paxospb.Entry{Cmd: make([]byte, 16)})
	q.get()
	q.get()
	tq := q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd == nil {
			t.Errorf("data unexpectedly freed")
		}
	}
}

func TestLazyFreeCanBeUsed(t *testing.T) {
	q := newEntryQueue(5, 1)
	q.add(paxospb.Entry{Cmd: make([]byte, 16)})
	q.add(paxospb.Entry{Cmd: make([]byte, 16)})
	q.add(paxospb.Entry{Cmd: make([]byte, 16)})
	q.get()
	q.get()
	tq := q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd != nil {
			t.Errorf("data unexpectedly not freed")
		}
	}
}

func TestLazyFreeCycleCanBeSet(t *testing.T) {
	q := newEntryQueue(5, 6)
	q.add(paxospb.Entry{Cmd: make([]byte, 16)})
	q.add(paxospb.Entry{Cmd: make([]byte, 16)})
	q.add(paxospb.Entry{Cmd: make([]byte, 16)})
	q.get()
	q.get()
	tq := q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd == nil {
			t.Errorf("data unexpectedly freed")
		}
	}
	q.get()
	q.get()
	tq = q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd == nil {
			t.Errorf("data unexpectedly freed")
		}
	}
	q.get()
	q.get()
	tq = q.targetQueue()
	for i := 0; i < 3; i++ {
		if tq[i].Cmd != nil {
			t.Errorf("data not freed at the expected cycle")
		}
	}
}
func TestEntryQueueAllowEntriesToBeAdded(t *testing.T) {
	q := newEntryQueue(5, 0)
	for i := uint64(0); i < 5; i++ {
		ok, stopped := q.add(paxospb.Entry{InstanceID: uint64(i + 1)})
		if !ok || stopped {
			t.Errorf("failed to add new entry")
		}
		if q.idx != i+1 {
			t.Errorf("idx %d, want %d", q.idx, i+1)
		}
		var r []paxospb.Entry
		if q.leftInWrite {
			r = q.left
		} else {
			r = q.right
		}
		if r[i].InstanceID != uint64(i+1) {
			t.Errorf("InstanceID %d, want %d", r[i].InstanceID, uint64(i+1))
		}
	}
}

func TestEntryQueueAllowAddedEntriesToBeReturned(t *testing.T) {
	q := newEntryQueue(5, 0)
	for i := 0; i < 3; i++ {
		ok, stopped := q.add(paxospb.Entry{InstanceID: uint64(i + 1)})
		if !ok || stopped {
			t.Errorf("failed to add new entry")
		}
	}
	r := q.get()
	if len(r) != 3 {
		t.Errorf("len %d, want %d", len(r), 3)
	}
	if q.idx != 0 {
		t.Errorf("idx %d, want %d", q.idx, 0)
	}
	// check whether we can keep adding entries as long as we keep getting
	// previously written entries.
	expectedInstanID := uint64(1)
	q = newEntryQueue(5, 0)
	for i := 0; i < 1000; i++ {
		ok, stopped := q.add(paxospb.Entry{InstanceID: uint64(i + 1)})
		if !ok || stopped {
			t.Errorf("failed to add new entry")
		}
		if q.idx == q.size {
			r := q.get()
			if len(r) != 5 {
				t.Errorf("len %d, want %d", len(r), 5)
			}
			for _, e := range r {
				if e.InstanceID != expectedInstanID {
					t.Errorf("index %d, expected %d", e.InstanceID, expectedInstanID)
				}
				expectedInstanID++
			}
		}
	}
}

func TestGroupCanBeSetAsReady(t *testing.T) {
	rc := newReadyGroup()
	if len(rc.ready) != 0 {
		t.Errorf("ready map not empty")
	}
	rc.setGroupReady(1)
	rc.setGroupReady(2)
	rc.setGroupReady(2)
	if len(rc.ready) != 2 {
		t.Errorf("ready map sz %d, want 2", len(rc.ready))
	}
	_, ok := rc.ready[1]
	if !ok {
		t.Errorf("group 1 not set as ready")
	}
	_, ok = rc.ready[2]
	if !ok {
		t.Errorf("group 2 not set as ready")
	}
}

func TestReadyGroupCanBeReturnedAndCleared(t *testing.T) {
	rc := newReadyGroup()
	if len(rc.ready) != 0 {
		t.Errorf("ready map not empty")
	}
	rc.setGroupReady(1)
	rc.setGroupReady(2)
	rc.setGroupReady(2)
	if len(rc.ready) != 2 {
		t.Errorf("ready map sz %d, want 2", len(rc.ready))
	}
	r := rc.getReadyGroups()
	if len(r) != 2 {
		t.Errorf("ready map sz %d, want 2", len(r))
	}
	if len(rc.ready) != 0 {
		t.Errorf("group ready map not cleared")
	}
	r = rc.getReadyGroups()
	if len(r) != 0 {
		t.Errorf("group ready map not cleared")
	}
	rc.setGroupReady(4)
	r = rc.getReadyGroups()
	if len(r) != 1 {
		t.Errorf("group ready not set")
	}
}

func TestWorkReadyCanBeCreated(t *testing.T) {
	wr := newWorkReady(4)
	if len(wr.readyMapList) != 4 || len(wr.readyChList) != 4 {
		t.Errorf("unexpected ready list len")
	}
	if wr.count != 4 {
		t.Errorf("unexpected count value")
	}
}

func TestPartitionerWorksAsExpected(t *testing.T) {
	wr := newWorkReady(4)
	p := wr.getPartitioner()
	vals := make(map[uint64]struct{})
	for i := uint64(0); i < uint64(128); i++ {
		idx := p.GetPartitionID(i)
		vals[idx] = struct{}{}
	}
	if len(vals) != 4 {
		t.Errorf("unexpected partitioner outcome")
	}
}

func TestWorkCanBeSetAsReady(t *testing.T) {
	wr := newWorkReady(4)
	select {
	case <-wr.waitCh(1):
		t.Errorf("ready signaled")
	case <-wr.waitCh(2):
		t.Errorf("ready signaled")
	case <-wr.waitCh(3):
		t.Errorf("ready signaled")
	case <-wr.waitCh(4):
		t.Errorf("ready signaled")
	default:
	}
	wr.groupReady(0)
	select {
	case <-wr.waitCh(1):
	case <-wr.waitCh(2):
		t.Errorf("ready signaled")
	case <-wr.waitCh(3):
		t.Errorf("ready signaled")
	case <-wr.waitCh(4):
		t.Errorf("ready signaled")
	default:
		t.Errorf("ready not signaled")
	}
	wr.groupReady(9)
	select {
	case <-wr.waitCh(1):
		t.Errorf("ready signaled")
	case <-wr.waitCh(2):
	case <-wr.waitCh(3):
		t.Errorf("ready signaled")
	case <-wr.waitCh(4):
		t.Errorf("ready signaled")
	default:
		t.Errorf("ready not signaled")
	}
}

func TestReturnedReadyMapContainsReadyGroupID(t *testing.T) {
	wr := newWorkReady(4)
	wr.groupReady(0)
	wr.groupReady(4)
	wr.groupReady(129)
	ready := wr.getReadyMap(1)
	if len(ready) != 2 {
		t.Errorf("unexpected ready map size, sz: %d", len(ready))
	}
	_, ok := ready[0]
	_, ok2 := ready[4]
	if !ok || !ok2 {
		t.Errorf("missing group id")
	}
	ready = wr.getReadyMap(2)
	if len(ready) != 1 {
		t.Errorf("unexpected ready map size")
	}
	_, ok = ready[129]
	if !ok {
		t.Errorf("missing group id")
	}
	ready = wr.getReadyMap(3)
	if len(ready) != 0 {
		t.Errorf("unexpected ready map size")
	}
}
