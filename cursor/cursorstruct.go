package cursor

import "scanning/lru"

type Cursor struct {
	memPointers []*interface{}
	memIndex    int
	lruPointer  *lru.LRUCache
}

func (c *Cursor) MemPointers() []*interface{} {
	return c.memPointers
}

func (c *Cursor) SetMemPointers(memPointers []*interface{}) {
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

func NewCursor(memPointers []*interface{}, memIndex int, lruPointer *lru.LRUCache) *Cursor {
	return &Cursor{memPointers: memPointers, memIndex: memIndex, lruPointer: lruPointer}
}
