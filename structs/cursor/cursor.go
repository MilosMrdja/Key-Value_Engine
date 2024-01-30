package cursor

import (
	"sstable/LSM"
	"sstable/lru"
	"sstable/mem/memtable/hash/hashmem"
)

type Cursor struct {
	memPointers []hashmem.Memtable
	maxMem      int
	memIndex    int
	lruPointer  *lru.LRUCache

	compress1 bool
	compress2 bool
	oneFile   bool
	N         int
	M         int
	numTables int
}

func (c *Cursor) Compress1() bool {
	return c.compress1
}

func (c *Cursor) SetCompress1(compress1 bool) {
	c.compress1 = compress1
}

func (c *Cursor) Compress2() bool {
	return c.compress2
}

func (c *Cursor) SetCompress2(compress2 bool) {
	c.compress2 = compress2
}

func (c *Cursor) OneFile() bool {
	return c.oneFile
}

func (c *Cursor) SetOneFile(oneFile bool) {
	c.oneFile = oneFile
}

func (c *Cursor) MaxMem() int {
	return c.maxMem
}

func (c *Cursor) SetMaxMem(maxMem int) {
	c.maxMem = maxMem
}

func (c *Cursor) MemPointers() []hashmem.Memtable {
	return c.memPointers
}

func (c *Cursor) SetMemPointers(memPointers []hashmem.Memtable) {
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

func (c *Cursor) AddToMemtable(key string, value []byte) bool {

	if c.memPointers[c.memIndex].IsReadOnly() {
		c.memIndex = (c.memIndex + 1) % len(c.memPointers)
	}
	if c.memPointers[c.memIndex].IsReadOnly() {
		c.memIndex = (c.memIndex - 1 + len(c.memPointers)) % len(c.memPointers)
		c.memPointers[c.memIndex].SendToSSTable(c.Compress1(), c.Compress2(), c.OneFile(), c.N, c.M)
		LSM.CompactSstable(c.numTables, c.Compress1(), c.Compress2(), c.OneFile())
	}
	return true
}
