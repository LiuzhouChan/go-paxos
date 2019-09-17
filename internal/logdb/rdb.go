package logdb

import (
	"encoding/binary"
	"errors"
	"math"

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
	firstInstanceID := entries[0].AcceptorState.InstanceID
	lastInstanceID := entries[len(entries)-1].AcceptorState.InstanceID

	key := ctx.GetKey()

	for i := firstInstanceID; i <= lastInstanceID; i++ {
		data := ctx.GetValueBuffer(uint64(entries[0].SizeUpperLimit()))
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
	firstKey.SetEntryKey(groupID, nodeID, low)
	lastKey.SetEntryKey(groupID, nodeID, high)
	op := func(key []byte, data []byte) (bool, error) {
		var e paxospb.Entry
		if err := e.Unmarshal(data); err != nil {
			panic(err)
		}
		ents = append(ents, e)
		return true, nil
	}
	r.kvs.IterateValue(firstKey.Key(), lastKey.Key(), false, op)
	return ents, nil
}

func (r *RDB) getEntryRange(groupID uint64, nodeID uint64,
	lastInstanceID uint64) (uint64, uint64, error) {
	maxInstanceID, err := r.readMaxInstance(groupID, nodeID)
	if err == paxosio.ErrNoSavedLog {
		return lastInstanceID, 0, nil
	}
	if err != nil {
		panic(err)
	}
	firstKey := r.keys.get()
	lastKey := r.keys.get()
	defer firstKey.Release()
	defer lastKey.Release()
	firstKey.SetEntryKey(groupID, nodeID, lastInstanceID)
	lastKey.SetEntryKey(groupID, nodeID, maxInstanceID+1)
	firstInstanceID := uint64(0)
	length := uint64(0)
	op := func(key []byte, data []byte) (bool, error) {
		var e paxospb.Entry
		if err := e.Unmarshal(data); err != nil {
			panic(err)
		}
		if e.AcceptorState.InstanceID >= lastInstanceID &&
			e.AcceptorState.InstanceID <= maxInstanceID {
			length++
			if firstInstanceID == uint64(0) {
				firstInstanceID = e.AcceptorState.InstanceID
			}
		}
		return true, nil
	}
	r.kvs.IterateValue(firstKey.Key(), lastKey.Key(), false, op)
	return firstInstanceID, length, nil
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

func (r *RDB) readState(groupID uint64, nodeID uint64) (*paxospb.State, error) {
	ko := r.keys.get()
	defer ko.Release()
	ko.SetStateKey(groupID, nodeID)
	hs := &paxospb.State{}
	if err := r.kvs.GetValue(ko.Key(), func(data []byte) error {
		if len(data) == 0 {
			return paxosio.ErrNoSavedLog
		}
		if err := hs.Unmarshal(data); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return hs, nil
}

func (r *RDB) recordState(groupID uint64, nodeID uint64,
	st paxospb.State, wb IWriteBatch, ctx paxosio.IContext) {
	if paxospb.IsEmptyState(st) {
		return
	}
	if !r.cs.setState(groupID, nodeID, st) {
		return
	}
	data := ctx.GetValueBuffer(uint64(st.Size()))
	ms, err := st.MarshalTo(data)
	if err != nil {
		panic(err)
	}
	data = data[:ms]
	ko := ctx.GetKey()
	ko.SetStateKey(groupID, nodeID)
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

func (r *RDB) readPaxosState(groupID uint64, nodeID uint64,
	lastInstanceID uint64) (*paxosio.PaxosState, error) {
	firstInstanceID, length, err := r.getEntryRange(groupID, nodeID, lastInstanceID)
	if err != nil {
		return nil, err
	}
	state, err := r.readState(groupID, nodeID)
	if err != nil {
		return nil, err
	}
	rs := &paxosio.PaxosState{
		State:           state,
		FirstInstanceID: firstInstanceID,
		EntryCount:      length,
	}
	return rs, nil
}

func (r *RDB) savePaxosState(updates []paxospb.Update, ctx paxosio.IContext) error {
	wb := r.kvs.GetWriteBatch(ctx)
	for _, ud := range updates {
		r.recordState(ud.GroupID, ud.NodeID, ud.State, wb, ctx)
	}
	r.saveEntries(updates, wb, ctx)
	if wb.Count() > 0 {
		return r.kvs.CommitWriteBatch(wb)
	}
	return nil
}

func (r *RDB) saveEntries(updates []paxospb.Update,
	wb IWriteBatch, ctx paxosio.IContext) {
	if len(updates) == 0 {
		return
	}
	for _, ud := range updates {
		groupID := ud.GroupID
		nodeID := ud.NodeID
		if len(ud.EntriesToSave) > 0 {
			mi := r.recordEntries(wb, groupID, nodeID, ctx, ud.EntriesToSave)
			if mi > 0 {
				r.setMaxInstanceID(wb, ud, mi, ctx)
			}
		}
	}
}

func (r *RDB) setMaxInstanceID(wb IWriteBatch, ud paxospb.Update,
	maxInstanceID uint64, ctx paxosio.IContext) {
	r.cs.setMaxInstance(ud.GroupID, ud.NodeID, maxInstanceID)
	r.recordMaxInstance(wb, ud.GroupID, ud.NodeID, maxInstanceID, ctx)
}

func (r *RDB) listNodeInfo() ([]paxosio.NodeInfo, error) {
	firstKey := newKey(bootstrapKeySize, nil)
	lastKey := newKey(bootstrapKeySize, nil)
	firstKey.setBootstrapKey(0, 0)
	lastKey.setBootstrapKey(math.MaxUint64, math.MaxUint64)
	ni := make([]paxosio.NodeInfo, 0)
	op := func(key []byte, data []byte) (bool, error) {
		gid, nid := parseNodeInfoKey(key)
		ni = append(ni, paxosio.GetNodeInfo(gid, nid))
		return true, nil
	}
	r.kvs.IterateValue(firstKey.Key(), lastKey.Key(), true, op)
	return ni, nil
}
