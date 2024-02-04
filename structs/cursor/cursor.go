package cursor

import (
	"encoding/binary"
	"sstable/LSM"
	"sstable/lru"
	"sstable/mem/memtable/btree/btreemem"
	"sstable/mem/memtable/datatype"
	"sstable/mem/memtable/hash/hashmem"
	"sstable/mem/memtable/hash/hashstruct"
	"sstable/mem/memtable/skiplist/skiplistmem"
	"sstable/wal_implementation"
	"time"
)

type Cursor struct {
	memPointers []hashmem.Memtable //lista pokazivaca na memtabele
	maxMem      int                // maksimalan Broj memtabela
	memIndex    int                // broj metabele koja je trenutno aktivna
	lruPointer  *lru.LRUCache      // pokazivac na kes

	compress1              bool   // da li je ukljucena kompresija duzine
	compress2              bool   // da li je ukljucena kompresija sa recnikom
	oneFile                bool   // da li se SSTable cuva u jednom fajlu
	N                      int    // razudjenost Index-a
	M                      int    // razudjenost Summary-ja
	numTables              int    // broj SSTabela na nivou
	memCap                 int    //kapacitet memtabele
	compType               string //koja kompakcija se koristi
	maxSSTLevel            int    //maksimalan broj nivoa za SStable
	levelPlus              int
	bloomFilterProbability float64
}

func (c *Cursor) BloomFilterProbability() float64 {
	return c.bloomFilterProbability
}

func (c *Cursor) SetBloomFilterProbability(bloomFilterProbability float64) {
	c.bloomFilterProbability = bloomFilterProbability
}

func NewCursor(memType string, maxMem int, lruPointer *lru.LRUCache, compress1 bool, compress2 bool, oneFile bool, n int, m int, numTables int, memCap int, compType string, maxSSTLevel, levelPlus int, bloomFilterProbability float64) *Cursor {
	memPointers := make([]hashmem.Memtable, maxMem)
	for i := 0; i < maxMem; i++ {
		if memType == "hash" {
			memPointers[i] = hashstruct.CreateHashMemtable(memCap)
		} else if memType == "skipl" {
			memPointers[i] = skiplistmem.CreateSkipListMemtable(memCap)
		} else if memType == "btree" {
			memPointers[i] = btreemem.NewBTreeMemtable(memCap)
		} else {
			memPointers[i] = btreemem.NewBTreeMemtable(memCap)
		}

	}

	return &Cursor{
		memPointers:            memPointers,
		maxMem:                 maxMem,
		memIndex:               0,
		lruPointer:             lruPointer,
		compress1:              compress1,
		compress2:              compress2,
		oneFile:                oneFile,
		N:                      n,
		M:                      m,
		numTables:              numTables,
		memCap:                 memCap,
		compType:               compType,
		maxSSTLevel:            maxSSTLevel,
		levelPlus:              levelPlus,
		bloomFilterProbability: bloomFilterProbability,
	}
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

func (c *Cursor) AddToMemtable(key string, value []byte, time time.Time, wal *wal_implementation.WriteAheadLog) bool {
	var full bool
	full = false

	//find, _ := c.memPointers[c.memIndex].GetElement(key)
	//if !find {
	//
	//} else {
	//	c.memPointers[c.memIndex].UpdateElement(key, value, time)
	//
	//}
	if c.memPointers[c.memIndex].IsReadOnly() {
		c.memIndex = (c.memIndex + 1) % len(c.memPointers)
		if c.memPointers[c.memIndex].IsReadOnly() {
			c.memIndex = (c.memIndex - 1 + c.maxMem) % c.maxMem
			c.memPointers[c.memIndex].SendToSSTable(c.Compress1(), c.Compress2(), c.OneFile(), c.N, c.M, c.maxSSTLevel, c.bloomFilterProbability)
			LSM.CompactSstable(c.numTables, c.BloomFilterProbability(), c.Compress1(), c.Compress2(), c.OneFile(), c.N, c.M, c.memCap, c.compType, c.maxSSTLevel, c.levelPlus)
			//Salje se signal u WAL da je memtable upisana na disk
			err := wal.DeleteMemTable()
			if err != nil {
				return false
			}
			full = c.memPointers[c.memIndex].AddElement(key, value, time)
		}
	} else {
		full = c.memPointers[c.memIndex].AddElement(key, value, time)
	}

	//ako se memtable popunio salje se signal u WAL
	if full {
		c.memIndex = (c.memIndex + 1) % c.maxMem
		err := wal.EndMemTable()
		if err != nil {
			return false
		}
	}

	return true
}

func (c *Cursor) findElement(key string) (int, bool) {

	find := false
	j := c.memIndex
	for true {
		find, _ = c.memPointers[j].GetElement(key)
		if find {
			break
		}
		j = (j - 1 + c.maxMem) % c.maxMem
		if j == c.memIndex {
			break
		}

	}
	return j, find
}

func (c *Cursor) GetElement(key string) ([]byte, bool) {

	var value *datatype.DataType

	j, find := c.findElement(key)

	if find {
		_, value = c.memPointers[j].GetElement(key)
		if value.IsDeleted() {
			return []byte(""), true
		}
		return value.GetData(), find
	}
	return nil, false

}

func (c *Cursor) DeleteElement(key string, time time.Time) bool {
	return c.memPointers[c.memIndex].DeleteElement(key, time)

}

func (c *Cursor) Fill(wal *wal_implementation.WriteAheadLog) {
	for true {
		rec, err := wal.ReadRecord()
		if err != "" {
			if err == "NO MORE RECORDS" {
				break
			}
		}
		if err != "CRC FAILED!" {
			nano := int64(binary.BigEndian.Uint64(rec.Timestamp[8:]))
			timestamp := time.Unix(nano, 0)
			c.AddToMemtable(rec.Key, rec.Value, timestamp, wal)
		}
	}
}
