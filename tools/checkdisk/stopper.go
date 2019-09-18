package main

import (
	"log"
	"sync"

	"github.com/LiuzhouChan/go-paxos/internal/utils/envutil"
	"github.com/LiuzhouChan/go-paxos/internal/utils/lang"
	"github.com/LiuzhouChan/go-paxos/logger"
)

var (
	plog = logger.GetLogger("checkdisk")
)

// Stopper is a manager struct for managing worker goroutines. It is modified
// from an early version of the stopper struct found in CockroachDB's codebase.
type Stopper struct {
	shouldStopC chan struct{}
	wg          sync.WaitGroup
	debug       bool
}

// NewStopper return a new Stopper instance.
func NewStopper() *Stopper {
	s := &Stopper{
		shouldStopC: make(chan struct{}),
		debug:       envutil.GetBoolEnvVarOrDefault("LEAKTEST", false),
	}

	return s
}

// RunWorker creates a new goroutine and invoke the f func in that new
// worker goroutine.
func (s *Stopper) RunWorker(f func()) {
	s.runWorker(f, "")
}

// RunPWorker creates a new goroutine and invoke the f func with the specified
// parameter p in that new worker goroutine.
func (s *Stopper) RunPWorker(f func(arg interface{}), p interface{}) {
	s.runPWorker(f, p)
}

// RunNamedWorker creates a new gorotuine and invoke the f func in that
// new worker goroutine. The specified name is to identify the worker,
// it is typically used for debugging purposes.
func (s *Stopper) RunNamedWorker(f func(), name string) {
	s.runWorker(f, name)
}

func (s *Stopper) runPWorker(f func(arg interface{}), p interface{}) {
	s.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				plog.Errorf("worker is panicing, %v", r)
				panic(r)
			}
		}()
		f(p)
		s.wg.Done()
	}()
}

func (s *Stopper) runWorker(f func(), name string) {
	s.wg.Add(1)
	var gid uint64
	go func() {
		defer func() {
			if r := recover(); r != nil {
				plog.Errorf("worker is panicing, %v", r)
				panic(r)
			}
		}()
		if s.debug {
			gid = lang.GetGIDForDebugOnly()
			log.Printf("goroutine %d started, name %s", gid, name)
		}
		f()
		s.wg.Done()
		if s.debug {
			log.Printf("goroutine %d stopped, name %s", gid, name)
		}
	}()
}

// ShouldStop returns a chan struct{} used for indicating whether the
// Stop() function has been called on Stopper.
func (s *Stopper) ShouldStop() chan struct{} {
	return s.shouldStopC
}

// Wait waits on the internal sync.WaitGroup. It only return when all
// managed worker goroutines are ready to return and called
// sync.WaitGroup.Done() on the internal sync.WaitGroup.
func (s *Stopper) Wait() {
	s.wg.Wait()
}

// Stop signals all managed worker goroutines to stop and wait for them
// to actually stop.
func (s *Stopper) Stop() {
	close(s.shouldStopC)
	s.wg.Wait()
}

// Close closes the internal shouldStopc chan struct{} to signal all
// worker goroutines that they should stop.
func (s *Stopper) Close() {
	close(s.shouldStopC)
}
