package ipaxos

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
