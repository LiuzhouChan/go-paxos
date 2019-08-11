package paxos

import (
	"sort"
	"sync"
	"time"
)

const (
	maxSampleCount = 50000
)

// a moving window for the records
type sample struct {
	mu        sync.Mutex
	sampled   bool
	startTime time.Time
	samples   []int64
}

func newSample() *sample {
	return &sample{samples: make([]int64, 0)}
}

func (s *sample) start() {
	if !s.sampled {
		return
	}
	s.startTime = time.Now()
}

func (s *sample) end() {
	if !s.sampled {
		return
	}
	s.addSample(s.startTime)
}

func (s *sample) record(start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addSample(start)
}

func (s *sample) addSample(start time.Time) {
	cost := time.Since(start).Nanoseconds() / 1000
	s.samples = append(s.samples, cost)
	if len(s.samples) >= maxSampleCount {
		s.samples = s.samples[1:]
	}
}

func (s *sample) median() int64 {
	return s.percentile(50.0)
}

func (s *sample) p999() int64 {
	return s.percentile(99.9)
}

func (s *sample) p99() int64 {
	return s.percentile(99.0)
}

func (s *sample) percentile(p float64) int64 {
	if len(s.samples) == 0 {
		return 0
	}
	sort.Slice(s.samples, func(i int, j int) bool {
		return s.samples[i] < s.samples[j]
	})
	index := (p / 100) * float64(len(s.samples))
	if index == float64(int64(index)) {
		i := int(index)
		return s.samples[i-1]
	} else if index > 1 {
		i := int(index)
		return (s.samples[i] + s.samples[i-1]) / 2
	}
	return 0
}
