package paxos

import "github.com/LiuzhouChan/go-paxos/paxospb"

func newEntrySlice(ents []paxospb.Entry) []paxospb.Entry {
	var n []paxospb.Entry
	return append(n, ents...)
}

func min(x uint64, y uint64) uint64 {
	if x > y {
		return y
	}
	return x
}

func max(x uint64, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

// IsLocalMessageType returns a boolean value indicating whether the specified
// message type is a local message type.
func IsLocalMessageType(t paxospb.MessageType) bool {
	return isLocalMessageType(t)
}

func isLocalMessageType(t paxospb.MessageType) bool {
	return t == paxospb.LocalTick
}
