package logdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const (
	RDBTestDirectory = "rdb_test_dir_safe_to_delete"
)

func getNewTestDB(dir string, lldir string) paxosio.ILogDB {
	d := filepath.Join(RDBTestDirectory, dir)
	lld := filepath.Join(RDBTestDirectory, lldir)
	os.MkdirAll(d, 0777)
	os.MkdirAll(lld, 0777)
	db, err := OpenLogDB([]string{d}, []string{lld})
	if err != nil {
		panic(err)
	}
	return db
}

func deleteTestDB() {
	os.RemoveAll(RDBTestDirectory)
}

func runLogDBTest(t *testing.T, tf func(t *testing.T, db paxosio.ILogDB)) {
	defer leaktest.AfterTest(t)()
	dir := "db-dir"
	lldir := "wal-db-dir"
	db := getNewTestDB(dir, lldir)
	defer deleteTestDB()
	defer db.Close()
	tf(t, db)
}
