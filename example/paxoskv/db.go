package paxoskv

import (
	"errors"
	"sync"

	"github.com/LiuzhouChan/go-paxos/logger"
	"github.com/LiuzhouChan/go-paxos/statemachine"
)

var (
	plog = logger.GetLogger("paxoskv")
)

var (
	// ErrKeyNotExist means the key is not exist
	ErrKeyNotExist = errors.New("key not exists")
	//ErrKeyVersionConflict means the key version is conflict
	ErrKeyVersionConflict = errors.New("key version conflict")
)

// BytesDeepCopy ...
func BytesDeepCopy(b []byte) []byte {
	ret := make([]byte, 0)
	ret = append(ret, b...)
	return ret
}

type kvMap struct {
	mu sync.Mutex
	kv map[string][]byte
}

func (kv *kvMap) get(key string) ([]byte, uint64, error) {
	if len(key) == 0 {
		panic("empty key is not allowed")
	}
	v, ok := kv.kv[key]
	if !ok {
		return nil, 0, ErrKeyNotExist
	}
	var kvdata KVData
	err := kvdata.Unmarshal(v)
	if err != nil {
		panic(err)
	}
	if kvdata.Isdeleted {
		return nil, kvdata.Version, ErrKeyNotExist
	}
	return kvdata.Value, kvdata.Version, nil
}

func (kv *kvMap) set(key string, value []byte, version uint64) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, serverVersion, err := kv.get(key)
	if err != nil && err != ErrKeyNotExist {
		return err
	}
	if serverVersion != version {
		return ErrKeyVersionConflict
	}
	serverVersion++
	kvdata := &KVData{
		Version:   serverVersion,
		Isdeleted: false,
		Value:     BytesDeepCopy(value),
	}
	data, err := kvdata.Marshal()
	if err != nil {
		panic(err)
	}
	kv.kv[key] = data
	return nil
}

func (kv *kvMap) del(key string, version uint64) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	serverValue, serverVersion, err := kv.get(key)
	if err != nil && err != ErrKeyNotExist {
		return err
	}
	if serverVersion != version {
		return ErrKeyVersionConflict
	}
	serverVersion++
	kvdata := &KVData{
		Version:   serverVersion,
		Isdeleted: true,
		Value:     BytesDeepCopy(serverValue),
	}
	data, err := kvdata.Marshal()
	if err != nil {
		panic(err)
	}
	kv.kv[key] = data
	return nil
}

// DB is the IStateMachine implementation used in the
// paxoskv example.
type DB struct {
	GroupID uint64
	NodeID  uint64
	KVMap   *kvMap
}

// NewDB creates a new DB instance.
func NewDB(groupID uint64, nodeID uint64) statemachine.IStateMachine {
	d := &DB{
		GroupID: groupID,
		NodeID:  nodeID,
		KVMap: &kvMap{
			kv: make(map[string][]byte),
		},
	}
	return d
}

// Close closes the DB instance.
func (d *DB) Close() {}

// GetHash returns the state machine hash.
func (d *DB) GetHash() uint64 {
	return 1
}

// Update updates the DB instance.
func (d *DB) Update(data []byte) uint64 {
	var req KVRequest
	err := req.Unmarshal(data)
	if err != nil {
		panic(err)
	}
	if req.Operator == KVRequest_READ {
		plog.Panicf("invalid operator type in update %v", KVRequest_READ)
	} else if req.Operator == KVRequest_WRITE {
		err = d.KVMap.set(req.Key, req.Value, req.Version)
	} else if req.Operator == KVRequest_DELETE {
		err = d.KVMap.del(req.Key, req.Version)
	}
	if err == ErrKeyVersionConflict {
		return uint64(KVResponse_VERSION_CONFLICT)
	}
	return uint64(KVResponse_OK)
}

// Lookup ...
func (d *DB) Lookup(key []byte) []byte {
	var req KVRequest
	err := req.Unmarshal(key)
	if err != nil {
		panic(err)
	}
	if req.Operator != KVRequest_READ {
		plog.Panicf("the operator in loopup should be KVRequest_READ")
	}
	value, version, err := d.KVMap.get(req.Key)
	res := KVResponse{
		Data: KVData{
			Value:   BytesDeepCopy(value),
			Version: version,
		},
	}
	if err == ErrKeyNotExist {
		res.Code = KVResponse_KEY_NOT_EXIST
	} else {
		res.Code = KVResponse_OK
	}
	data, err := res.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}
