package logdb

import (
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxospb"
)

func TestRDBContextCanBeCreated(t *testing.T) {
	ctx := newRDBContext(128, newSimpleWriteBatch())
	if ctx.key == nil || len(ctx.val) != 128 {
		t.Errorf("unexpected key/value")
	}
	if ctx.wb.Count() != 0 {
		t.Errorf("wb not empty")
	}
}

func TestRDBContextCaBeDestroyed(t *testing.T) {
	ctx := newRDBContext(128, newSimpleWriteBatch())
	ctx.Destroy()
}

func TestRDBContextCaBeReset(t *testing.T) {
	ctx := newRDBContext(128, newSimpleWriteBatch())
	ctx.wb.Put([]byte("key"), []byte("val"))
	if ctx.wb.Count() != 1 {
		t.Errorf("unexpected count")
	}
	ctx.Reset()
	if ctx.wb.Count() != 0 {
		t.Errorf("wb not cleared")
	}
}

func TestGetValueBuffer(t *testing.T) {
	ctx := newRDBContext(128, newSimpleWriteBatch())
	buf := ctx.GetValueBuffer(100)
	if cap(buf) != 128 {
		t.Errorf("didn't return the default buffer")
	}
	buf = ctx.GetValueBuffer(1024)
	if cap(buf) != 1024 {
		t.Errorf("didn't return a new buffer")
	}
}

func TestGetUpdates(t *testing.T) {
	ctx := newRDBContext(128, newSimpleWriteBatch())
	v := ctx.GetUpdates()
	if cap(v) != updateSliceLen {
		t.Errorf("unexpected updates cap")
	}
	if len(v) != 0 {
		t.Errorf("unexpected len")
	}
	v2 := append(v, paxospb.Update{})
	if len(v2) != 1 {
		t.Errorf("unexpected len")
	}
	v = ctx.GetUpdates()
	if cap(v) != updateSliceLen {
		t.Errorf("unexpected updates cap")
	}
	if len(v) != 0 {
		t.Errorf("unexpected len")
	}
}
