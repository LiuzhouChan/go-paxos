package tests

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/LiuzhouChan/go-paxos/internal/tests/kvpb"
	"github.com/LiuzhouChan/go-paxos/statemachine"
	"github.com/gogo/protobuf/proto"
)

// random delays
func generateRandomDelay() {
	v := rand.Uint64()
	if v%10000 == 0 {
		time.Sleep(300 * time.Millisecond)
	} else if v%1000 == 0 {
		time.Sleep(100 * time.Millisecond)
	} else if v%100 == 0 {
		time.Sleep(10 * time.Millisecond)
	} else if v%20 == 0 {
		time.Sleep(2 * time.Millisecond)
	}
}

func getLargeRandomDelay() uint64 {
	// in IO error injection test, we don't want such delays
	ioei := os.Getenv("IOEI")
	if len(ioei) > 0 {
		return 0
	}
	v := rand.Uint64() % 100
	if v == 0 {
		return 30 * 1000
	}
	if v < 10 {
		return 1 * 1000
	}
	if v < 30 {
		return 500
	}
	if v < 50 {
		return 100
	}
	return 50
}

// KVTest is a in memory key-value store struct used for testing purposes.
// Note that both key/value are suppose to be valid utf-8 strings.
type KVTest struct {
	GroupID          uint64            `json:"-"`
	NodeID           uint64            `json:"-"`
	KVStore          map[string]string `json:"KVStore"`
	Count            uint64            `json:"Count"`
	Junk             []byte            `json:"Junk"`
	closed           bool
	aborted          bool `json:"-"`
	externalFileTest bool
	pbkvPool         *sync.Pool
}

// NewKVTest creates and return a new KVTest object.
func NewKVTest(groupID uint64, nodeID uint64) statemachine.IStateMachine {
	fmt.Println("kvtest with stoppable snapshot created")
	s := &KVTest{
		KVStore: make(map[string]string),
		GroupID: groupID,
		NodeID:  nodeID,
		Junk:    make([]byte, 3*1024),
	}
	v := os.Getenv("EXTERNALFILETEST")
	s.externalFileTest = len(v) > 0
	fmt.Printf("junk data inserted, external file test %t\n", s.externalFileTest)
	// write some junk data consistent across the cluster
	for i := 0; i < len(s.Junk); i++ {
		s.Junk[i] = 2
	}
	s.pbkvPool = &sync.Pool{
		New: func() interface{} {
			return &kvpb.PBKV{}
		},
	}
	return s
}

// Lookup performances local looks up for the sepcified data.
func (s *KVTest) Lookup(key []byte) []byte {
	if s.closed {
		panic("lookup called after Close()")
	}

	if s.aborted {
		panic("Lookup() called after abort set to true")
	}
	v, ok := s.KVStore[string(key)]
	generateRandomDelay()
	if ok {
		return []byte(v)
	}

	return []byte("")
}

// Update updates the object using the specified committed raft entry.
func (s *KVTest) Update(data []byte) uint64 {
	s.Count++
	if s.aborted {
		panic("update() called after abort set to true")
	}
	if s.closed {
		panic("update called after Close()")
	}
	generateRandomDelay()
	dataKv := s.pbkvPool.Get().(*kvpb.PBKV)
	err := proto.Unmarshal(data, dataKv)
	if err != nil {
		panic(err)
	}
	s.updateStore(dataKv.GetKey(), dataKv.GetVal())
	s.pbkvPool.Put(dataKv)

	return uint64(len(data))
}

// Close closes the IStateMachine instance
func (s *KVTest) Close() {
	s.closed = true
	log.Printf("%d:%dKVStore has been closed", s.GroupID, s.NodeID)
}

// GetHash returns a uint64 representing the current object state.
func (s *KVTest) GetHash() uint64 {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}

	hash := md5.New()
	if _, err = hash.Write(data); err != nil {
		panic(err)
	}
	md5sum := hash.Sum(nil)
	return binary.LittleEndian.Uint64(md5sum[:8])
}

func (s *KVTest) updateStore(key string, value string) {
	s.KVStore[key] = value
}
