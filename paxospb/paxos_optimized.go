package paxospb

// SizeUpperLimit returns the upper limit size of an entry.
func (m *PaxosMsg) SizeUpperLimit() int {
	l := 8 * 2
	l += 16 * 12
	l += 16
	l += len(m.Value)
	return l
}

// SizeUpperLimit returns the upper limit size of an entry.
func (m *MessageBatch) SizeUpperLimit() int {
	l := 0
	l += (16 * 3) + len(m.SourceAddress)
	for _, msg := range m.Requests {
		l += 16
		l += msg.SizeUpperLimit()
	}
	return l
}
