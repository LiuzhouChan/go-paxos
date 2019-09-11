package logdb

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func modifyLogDBContent(fp string) {
	idx := int64(0)
	f, err := os.OpenFile(fp, os.O_RDWR, 0755)
	defer f.Close()
	if err != nil {
		panic("failed to open the file")
	}
	located := false
	data := make([]byte, 4)
	for {
		_, err := f.Read(data)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic("read failed")
			}
		}
		if string(data) == "XXXX" {
			// got it
			located = true
			break
		}
		idx += 4
	}
	if !located {
		panic("failed to locate the data")
	}
	_, err = f.Seek(idx, 0)
	if err != nil {
		panic(err)
	}
	_, err = f.Write([]byte("YYYY"))
	if err != nil {
		panic(err)
	}
}

func sstFileToCorruptFilePath() []string {
	dp := filepath.Join(RDBTestDirectory, "db-dir", "logdb-3")
	fi, err := ioutil.ReadDir(dp)
	if err != nil {
		panic(err)
	}
	result := make([]string, 0)
	for _, v := range fi {
		if strings.HasSuffix(v.Name(), ".sst") {
			result = append(result, filepath.Join(dp, v.Name()))
		}
	}
	return result
}

// this is largely to check the rocksdb wrapper doesn't slightly swallow
// detected data corruption related errors
func testDiskDataCorruptionIsHandled(t *testing.T, f func(paxosio.ILogDB)) {
	dir := "db-dir"
	lldir := "wal-db-dir"
	db := getNewTestDB(dir, lldir)
	defer deleteTestDB()
	hs := paxospb.State{
		Commit: 100,
	}
	e1 := paxospb.Entry{
		Type: paxospb.ApplicationEntry,
		AcceptorState: paxospb.AcceptorState{
			InstanceID:    10,
			AccetpedValue: []byte("XXXXXXXXXXXXXXXXXXXXXXXX"),
		},
	}
	ud := paxospb.Update{
		EntriesToSave: []paxospb.Entry{e1},
		State:         hs,
		GroupID:       3,
		NodeID:        4,
	}
	for i := 0; i < 128; i++ {
		err := db.SavePaxosState([]paxospb.Update{ud}, newRDBContext(1, nil))
		if err != nil {
			t.Errorf("failed to save single de rec")
		}
	}
	db.Close()
	db = getNewTestDB(dir, lldir)
	db.Close()
	for _, fp := range sstFileToCorruptFilePath() {
		plog.Infof(fp)
		modifyLogDBContent(fp)
	}
	db = getNewTestDB(dir, lldir)
	defer db.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("didn't crash")
		}
	}()
	f(db)
}

func TestReadPaxosStateWithDiskCorruptionHandled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := func(fdb paxosio.ILogDB) {
		fdb.ReadPaxosState(3, 4, 0)
	}
	testDiskDataCorruptionIsHandled(t, f)
}

func TestIteratorWithDiskCorruptionHandled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := func(fdb paxosio.ILogDB) {
		rdb := fdb.(*ShardedRDB).shards[3]
		fk := rdb.keys.get()
		fk.SetEntryKey(3, 4, 10)
		iter := rdb.kvs.(*rocksdbKV).db.NewIterator(rdb.kvs.(*rocksdbKV).ro)
		iter.Seek(fk.key)
		for ; iteratorIsValid(iter); iter.Next() {
			plog.Infof("here")
			val := iter.Value()
			var e paxospb.Entry
			if err := e.Unmarshal(val.Data()); err != nil {
				panic(err)
			}
			plog.Infof(string(e.AcceptorState.AccetpedValue))
		}
	}
	testDiskDataCorruptionIsHandled(t, f)
}
