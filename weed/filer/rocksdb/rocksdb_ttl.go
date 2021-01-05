//+build rocksdb

package rocksdb

import (
	"time"

	"github.com/tecbot/gorocksdb"

	"github.com/chrislusf/seaweedfs/weed/filer"
)

type TTLFilter struct {
	skipLevel0 bool
}

func NewTTLFilter() gorocksdb.CompactionFilter {
	return &TTLFilter{
		skipLevel0: true,
	}
}

func (t *TTLFilter) Filter(level int, key, val []byte) (remove bool, newVal []byte) {
	// decode could be slow, causing write stall
	// level >0 sst can run compaction in parallel
	if t.skipLevel0 && level == 0 {
		return false, val
	}
	entry := filer.Entry{}
	if err := entry.DecodeAttributesAndChunks(val); err == nil {
		if entry.TtlSec == 0 ||
			entry.Crtime.Add(time.Duration(entry.TtlSec)*time.Second).Before(time.Now()) {
			return false, val
		}
	}
	return true, nil
}

func (t *TTLFilter) Name() string {
	return "TTLFilter"
}
