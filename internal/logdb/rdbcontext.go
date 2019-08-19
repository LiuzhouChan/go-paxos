package logdb

import (
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

const (
	updateSliceLen = 256
)

// rdbcontext is an IContext implementation suppose to be owned and used
// by a single thread throughout its life time.
type rdbcontext struct {
	valSize uint64
	// e       paxospb.Entry
	// l       paxospb.Entry
	key     *PooledKey
	val     []byte
	updates []paxospb.Update
	wb      IWriteBatch
}

// newRDBContext creates a new RDB context instance.
func newRDBContext(valSize uint64, wb IWriteBatch) *rdbcontext {
	ctx := &rdbcontext{
		valSize: valSize,
		key:     newKey(maxKeySize, nil),
		val:     make([]byte, valSize),
		updates: make([]paxospb.Update, 0, updateSliceLen),
		wb:      wb,
	}
	// ctx.lb.Entries = make([]pb.Entry, 0, batchSize)
	// ctx.eb.Entries = make([]pb.Entry, 0, batchSize)
	return ctx
}

func (c *rdbcontext) Destroy() {
	if c.wb != nil {
		c.wb.Destroy()
	}
}

func (c *rdbcontext) Reset() {
	if c.wb != nil {
		c.wb.Clear()
	}
}

func (c *rdbcontext) GetKey() paxosio.IReusableKey {
	return c.key
}

func (c *rdbcontext) GetValueBuffer(sz uint64) []byte {
	if sz <= c.valSize {
		return c.val
	}
	return make([]byte, sz)
}

func (c *rdbcontext) GetUpdates() []paxospb.Update {
	return c.updates
}

func (c *rdbcontext) GetWriteBatch() interface{} {
	return c.wb
}
