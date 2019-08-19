package logdb

import (
	"encoding/binary"
	"errors"

	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

// RDB is the struct used to manage rocksdb backed persistent Log stores.
type RDB struct {
	cs   *rdbcache
	keys *logdbKeyPool
	kvs  IKvStore
}

func openRDB(dir string, wal string) (*RDB, error) {
	kvs, err := newKVStore(dir, wal)
	if err != nil {
		return nil, err
	}
	return &RDB{
		cs:   newRDBCache(),
		keys: newLogdbKeyPool(),
		kvs:  kvs,
	}, nil
}

func (r *RDB) close() {
	if err := r.kvs.Close(); err != nil {
		panic(err)
	}
}

func (r *RDB) getWriteBatch() IWriteBatch {
	return r.kvs.GetWriteBatch(nil)
}

func (r *RDB) recordEntries(wb IWriteBatch, groupID, nodeID uint64,
	ctx paxosio.IContext, entries []paxospb.Entry) uint64 {
	if len(entries) == 0 {
		panic("empty entries")
	}
	firstInstanceID := entries[0].InstanceID
	lastInstanceID := entries[len(entries)-1].InstanceID

	//FIXME: set the buff size to Limit
	data := ctx.GetValueBuffer(1024)
	key := ctx.GetKey()

	for i := firstInstanceID; i <= lastInstanceID; i++ {
		key.SetEntryKey(groupID, nodeID, i)
		sz, err := entries[i-firstInstanceID].MarshalTo(data)
		if err != nil {
			panic(err)
		}
		data = data[:sz]
		wb.Put(key.Key(), data)
	}
	return lastInstanceID
}

func (r *RDB) getEntryFromDB(groupID, nodeID, instanceID uint64) (paxospb.Entry, bool) {
	var e paxospb.Entry
	key := r.keys.get()
	defer key.Release()
	key.SetEntryKey(groupID, nodeID, instanceID)
	if err := r.kvs.GetValue(key.Key(), func(data []byte) error {
		if len(data) == 0 {
			return errors.New("no such entry")
		}
		if err := e.Unmarshal(data); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		return e, false
	}
	return e, true
}

func (r *RDB) iterateEntrys(groupID, nodeID, low, high uint64) ([]paxospb.Entry, error) {
	maxInstance, err := r.readMaxInstance(groupID, nodeID)
	if err == paxosio.ErrNoSavedLog {
		return []paxospb.Entry{}, nil
	}
	if err != nil {
		panic(err)
	}
	if high > maxInstance+1 {
		high = maxInstance + 1
	}
	ents := make([]paxospb.Entry, 0)
	if low+1 == high {
		e, ok := r.getEntryFromDB(groupID, nodeID, low)
		if ok {
			ents = append(ents, e)
		}
		return ents, nil
	}
	firstKey := r.keys.get()
	lastKey := r.keys.get()
	defer firstKey.Release()
	defer lastKey.Release()
	op := func(key []byte, data []byte) (bool, error) {
		var e paxospb.Entry
		if err := e.Unmarshal(data); err != nil {
			panic(err)
		}
		ents := append(ents, e)
		return true, nil
	}
	r.kvs.IterateValue(firstKey.Key(), lastKey.Key(), false, op)
	return ents, nil
}

func (r *RDB) readMaxInstance(groupID, nodeID uint64) (uint64, error) {
	if v, ok := r.cs.getMaxInstance(groupID, nodeID); ok {
		return v, nil
	}
	ko := r.keys.get()
	defer ko.Release()
	ko.SetMaxInstanceKey(groupID, nodeID)
	maxInstance := uint64(0)
	if err := r.kvs.GetValue(ko.Key(), func(data []byte) error {
		if len(data) == 0 {
			return paxosio.ErrNoSavedLog
		}
		maxInstance = binary.BigEndian.Uint64(data)
		return nil
	}); err != nil {
		return 0, err
	}
	return maxInstance, nil
}

func (r *RDB) recordMaxInstance(wb IWriteBatch,
	groupID, nodeID, instanceID uint64, ctx paxosio.IContext) {
	data := ctx.GetValueBuffer(8)
	binary.BigEndian.PutUint64(data, instanceID)
	data = data[:8]
	ko := ctx.GetKey()
	ko.SetMaxInstanceKey(groupID, nodeID)
	wb.Put(ko.Key(), data)
}

func (r *RDB) saveBootstrapInfo(groupID, nodeID uint64,
	bootstrap paxospb.Bootstrap) error {
	data, err := bootstrap.Marshal()
	if err != nil {
		panic(err)
	}
	ko := newKey(maxKeySize, nil)
	ko.setBootstrapKey(groupID, nodeID)
	return r.kvs.SaveValue(ko.Key(), data)
}

func (r *RDB) getBootstrapInfo(groupID, nodeID uint64) (*paxospb.Bootstrap, error) {
	ko := newKey(maxKeySize, nil)
	ko.setBootstrapKey(groupID, nodeID)
	bootstrap := &paxospb.Bootstrap{}
	if err := r.kvs.GetValue(ko.Key(), func(data []byte) error {
		if len(data) == 0 {
			return paxosio.ErrNoBootstrapInfo
		}
		if err := bootstrap.Unmarshal(data); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return bootstrap, nil
}
