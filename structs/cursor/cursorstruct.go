package cursor

import (
	"scanning/lru"
	"scanning/mem/memtable/hash/hashmem"
)

type Cursor struct {
	memFlag     bool
	memPointers []*hashmem.Memtable

	memIndex   int
	lruPointer *lru.LRUCache
}

func (c *Cursor) MemFlag() bool {
	return c.memFlag
}

func (c *Cursor) SetMemFlag(memFlag bool) {
	c.memFlag = memFlag
}

func (c *Cursor) MemPointers() []*hashmem.Memtable {
	return c.memPointers
}

func (c *Cursor) SetMemPointers(memPointers []*hashmem.Memtable) {
	c.memPointers = memPointers
}

func (c *Cursor) MemIndex() int {
	return c.memIndex
}

func (c *Cursor) SetMemIndex(memIndex int) {
	c.memIndex = memIndex
}

func (c *Cursor) LruPointer() *lru.LRUCache {
	return c.lruPointer
}

func (c *Cursor) SetLruPointer(lruPointer *lru.LRUCache) {
	c.lruPointer = lruPointer
}

func NewCursor(memFlag bool, memPointers []*hashmem.Memtable, memIndex int, lruPointer *lru.LRUCache) *Cursor {
	return &Cursor{memFlag: memFlag, memPointers: memPointers, memIndex: memIndex, lruPointer: lruPointer}
}
