package paxos

import (
	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

var (
	entrySliceSize    = settings.Soft.InMemEntrySliceSize
	minEntrySliceSize = settings.Soft.MinEntrySliceFreeSize
)

type inMemory struct {
	entries          []paxospb.Entry
	markerInstanceID uint64
	saveTo           uint64
}

func newInMemory(lastInstanceID uint64) inMemory {
	return inMemory{
		markerInstanceID: lastInstanceID + 1,
		saveTo:           lastInstanceID,
	}
}

func (im *inMemory) checkMarkerInstanceID() {
	if len(im.entries) > 0 {
		if im.entries[0].AcceptorState.InstanceID != im.markerInstanceID {
			plog.Panicf("marker instance id %d, first instance id %d",
				im.markerInstanceID, im.entries[0].AcceptorState.InstanceID)
		}
	}
}

func (im *inMemory) getEntries(low uint64, high uint64) []paxospb.Entry {
	upperBound := im.markerInstanceID + uint64(len(im.entries))
	if low > high || low < im.markerInstanceID {
		plog.Panicf("invalid low value %d, high %d, marker instanace id %d",
			low, high, im.markerInstanceID)
	}
	if high > upperBound {
		plog.Panicf("invalid high value %d, upperBound %d", high, upperBound)
	}
	return im.entries[low-im.markerInstanceID : high-im.markerInstanceID]
}

func (im *inMemory) getLastInstanceID() (uint64, bool) {
	if len(im.entries) > 0 {
		return im.entries[len(im.entries)-1].AcceptorState.InstanceID, true
	}
	return 0, false
}

func (im *inMemory) commitUpdate(cu paxospb.UpdateCommit) {
	if cu.AppliedTo > 0 {
		im.saveLogTo(cu.AppliedTo)
	}
}

func (im *inMemory) entriesToSave() []paxospb.Entry {
	idx := im.saveTo + 1
	if idx-im.markerInstanceID > uint64(len(im.entries)) {
		plog.Infof("nothing to save %+v", im)
		return []paxospb.Entry{}
	}
	return im.entries[idx-im.markerInstanceID:]
}

func (im *inMemory) saveLogTo(instanceID uint64) {
	if instanceID < im.markerInstanceID {
		return
	}
	if len(im.entries) == 0 {
		return
	}
	if instanceID > im.entries[len(im.entries)-1].AcceptorState.InstanceID {
		return
	}
	im.saveTo = instanceID
}

func (im *inMemory) appliedLogTo(instanceID uint64) {
	if instanceID < im.markerInstanceID {
		return
	}
	if len(im.entries) == 0 {
		return
	}
	if instanceID > im.entries[len(im.entries)-1].AcceptorState.InstanceID {
		return
	}
	newMarkerInstanceID := instanceID
	im.entries = im.entries[newMarkerInstanceID-im.markerInstanceID:]
	im.markerInstanceID = newMarkerInstanceID
	im.resizeEntrySlice()
	im.checkMarkerInstanceID()
}

func (im *inMemory) resizeEntrySlice() {
	if cap(im.entries)-len(im.entries) < int(minEntrySliceSize) {
		old := im.entries
		im.entries = make([]paxospb.Entry, 0, entrySliceSize)
		im.entries = append(im.entries, old...)
	}
}

func (im *inMemory) merge(ents []paxospb.Entry) {
	firstInstanceID := ents[0].AcceptorState.InstanceID
	im.resizeEntrySlice()
	if firstInstanceID == im.markerInstanceID+uint64(len(im.entries)) {
		checkEntriesToAppend(im.entries, ents)
		im.entries = append(im.entries, ents...)
	} else if firstInstanceID <= im.markerInstanceID {
		im.markerInstanceID = firstInstanceID
		im.entries = newEntrySlice(ents)
		im.saveTo = firstInstanceID - 1
	} else {
		existing := im.getEntries(im.markerInstanceID, firstInstanceID)
		checkEntriesToAppend(existing, ents)
		im.entries = make([]paxospb.Entry, 0, len(existing)+len(ents))
		im.entries = append(im.entries, existing...)
		im.entries = append(im.entries, ents...)
		im.saveTo = min(im.saveTo, firstInstanceID-1)
	}
	im.checkMarkerInstanceID()
}

func checkEntriesToAppend(ents []paxospb.Entry, toAppend []paxospb.Entry) {
	if len(ents) == 0 || len(toAppend) == 0 {
		return
	}
	if ents[len(ents)-1].AcceptorState.InstanceID+1 != toAppend[0].AcceptorState.InstanceID {
		plog.Panicf("found a hole, last %d, first to append %d",
			ents[len(ents)-1].AcceptorState.InstanceID, toAppend[0].AcceptorState.InstanceID)
	}
}
