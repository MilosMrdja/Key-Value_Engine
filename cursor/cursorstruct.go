package cursor

import (
	"scanning/lru"
	"scanning/mem/memtable/btree/btreemem"
	"scanning/mem/memtable/hash/hashmem"
	"scanning/mem/memtable/skiplist/skiplistmem"
)

type Cursor struct {
	memFlag             bool
	hashMemPointers     []*hashmem.Memtable
	breeMemPointers     []*btreemem.BTreeMemtable
	skiplistMemPointers []*skiplistmem.SkipListMemtable
	memIndex            int
	lruPointer          *lru.LRUCache
}
