package rsm

import (
	"bytes"
	"crypto/rand"
	"os"
	"testing"
)

const (
	testSessionSize      uint64 = 4
	testPayloadSize      uint64 = 8
	testSnapshotFilename        = "testsnapshot_safe_to_delete.tmp"
)

func TestSnapshotWriterCanBeCreated(t *testing.T) {
	w, err := NewSnapshotWriter(testSnapshotFilename)
	if err != nil {
		t.Fatalf("failed to create snapshot writer %v", err)
	}
	defer os.RemoveAll(testSnapshotFilename)
	defer w.Close()
	pos, err := w.file.Seek(0, 1)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if uint64(pos) != SnapshotHeaderSize {
		t.Errorf("unexpected file position")
	}
}

func TestSaveHeaderSavesTheHeader(t *testing.T) {
	w, err := NewSnapshotWriter(testSnapshotFilename)
	if err != nil {
		t.Fatalf("failed to create snapshot writer %v", err)
	}
	defer os.RemoveAll(testSnapshotFilename)
	storeData := make([]byte, testPayloadSize)
	rand.Read(storeData)
	m, err := w.Write(storeData)
	if err != nil || m != len(storeData) {
		t.Fatalf("failed to write the store data")
	}
	storeChecksum := w.h.Sum(nil)
	if err := w.SaveHeader(uint64(m)); err != nil {
		t.Fatalf("%v", err)
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}
	r, err := NewSnapshotReader(testSnapshotFilename)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer r.Close()
	header, err := r.GetHeader()
	if err != nil {
		t.Fatalf("%v", err)
	}
	r.ValidateHeader(header)
	if header.DataStoreSize != uint64(len(storeData)) {
		t.Errorf("data store size mismatch")
	}
	if !bytes.Equal(header.PayloadChecksum, storeChecksum) {
		t.Errorf("data store checksum mismatch")
	}
}
