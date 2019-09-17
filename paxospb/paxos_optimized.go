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

// SizeUpperLimit returns the upper limit size of an entry.
func (m *Entry) SizeUpperLimit() int {
	l := 16 * 3
	l += m.AcceptorState.SizeUpperLimit()
	return l
}

// SizeUpperLimit returns the upper limit size of an entry.
func (m *AcceptorState) SizeUpperLimit() int {
	l := 16 * 6
	l += 16 * 2
	l += len(m.AccetpedValue)
	return l
}
