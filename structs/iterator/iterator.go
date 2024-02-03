package iterator

import (
	"sstable/cursor"
	"sstable/mem/memtable/datatype"
	"sstable/mem/memtable/hash/hashmem"
)

type RangeIterator struct {
	memTablePositions map[*hashmem.Memtable]int
	valRange          [2]string
	pageSize          int //velicina stranice
	pageNum           int //broj stranice
	pageStep          int //trenutna stranica
	iterStep          int //koliko unazad kod trazenja jednog
	iteratorChache    [][]datatype.DataType
}

func (i *RangeIterator) PageSize() int {
	return i.pageSize
}

func (i *RangeIterator) SetPageSize(pageSize int) {
	i.pageSize = pageSize
}

func (i *RangeIterator) PageNum() int {
	return i.pageNum
}

func (i *RangeIterator) SetPageNum(pageNum int) {
	i.pageNum = pageNum
}

func (i *RangeIterator) IteratorChache() [][]datatype.DataType {
	return i.iteratorChache
}

func (i *RangeIterator) SetIteratorChache(iteratorChache [][]datatype.DataType) {
	i.iteratorChache = iteratorChache
}

func (i *RangeIterator) ResetMemTableIndexes() {
	for k := range i.memTablePositions {
		i.memTablePositions[k] = 0
	}
}

func (i *RangeIterator) AllOnEnd() bool {
	j := 0
	for mem := range i.memTablePositions {
		a := *mem
		if a.GetMaxSize() == i.memTablePositions[mem] {
			j++
		}
	}
	if j == len(i.memTablePositions) {
		return true
	}
	return false
}

func (i *RangeIterator) ValRange() [2]string {
	return i.valRange
}

func (i *RangeIterator) SetValRange(valrange [2]string) {
	i.valRange = valrange
}

func (i *RangeIterator) MemTablePositions() map[*hashmem.Memtable]int {
	return i.memTablePositions
}

func (i *RangeIterator) IncrementMemTablePosition(memTablePtr *hashmem.Memtable) {
	a := *memTablePtr
	if a.GetMaxSize() == i.memTablePositions[memTablePtr] {
		i.memTablePositions[memTablePtr] = a.GetMaxSize()
	} else {
		i.memTablePositions[memTablePtr]++
	}

}
func NewRangeIterator(cursor *cursor.Cursor, valRange [2]string) *RangeIterator {
	memTablePositions := make(map[*hashmem.Memtable]int)
	cache := make([][]datatype.DataType, 0)
	for i := range cache {
		cache[i] = make([]datatype.DataType, 0)
	}
	pageSize := 0
	pageNum := 0
	for _, v := range cursor.MemPointers() {
		memTablePositions[&v] = 0
	}
	return &RangeIterator{memTablePositions: memTablePositions, valRange: valRange, pageSize: pageSize, pageNum: pageNum, iteratorChache: cache}
}

// ===============================================================================
type PrefixIterator struct {
	memTablePositions map[*hashmem.Memtable]int
	currPrefix        string
	pageSize          int
	pageNum           int
	pageStep          int
	iterStep          int
	iteratorChache    [][]datatype.DataType
}

func (i *PrefixIterator) PageSize() int {
	return i.pageSize
}

func (i *PrefixIterator) SetPageSize(pageSize int) {
	i.pageSize = pageSize
}

func (i *PrefixIterator) PageNum() int {
	return i.pageNum
}

func (i *PrefixIterator) SetPageNum(pageNum int) {
	i.pageNum = pageNum
}

func (i *PrefixIterator) IteratorChache() [][]datatype.DataType {
	return i.iteratorChache
}

func (i *PrefixIterator) SetIteratorChache(iteratorChache [][]datatype.DataType) {
	i.iteratorChache = iteratorChache
}

func NewPrefixIterator(cursor *cursor.Cursor, currPrefix string) *PrefixIterator {
	memTablePositions := make(map[*hashmem.Memtable]int)
	cache := make([][]datatype.DataType, 0)
	for i := range cache {
		cache[i] = make([]datatype.DataType, 0)
	}
	pageSize := 0
	pageNum := 0
	for _, v := range cursor.MemPointers() {
		memTablePositions[&v] = 0
	}
	for _, v := range cursor.MemPointers() {
		memTablePositions[&v] = 0
	}
	return &PrefixIterator{memTablePositions: memTablePositions, currPrefix: currPrefix, pageNum: pageNum, pageSize: pageSize, iteratorChache: cache}
}

func (i *PrefixIterator) ResetMemTableIndexes() {
	for k := range i.memTablePositions {
		i.memTablePositions[k] = 0
	}
}

func (i *PrefixIterator) AllOnEnd() bool {
	j := 0
	for mem := range i.memTablePositions {
		a := *mem
		if a.GetMaxSize() == i.memTablePositions[mem] {
			j++
		}
	}
	if j == len(i.memTablePositions) {
		return true
	}
	return false
}

func (i *PrefixIterator) CurrPrefix() string {
	return i.currPrefix
}

func (i *PrefixIterator) SetCurrPrefix(currPrefix string) {
	i.currPrefix = currPrefix
}

func (i *PrefixIterator) MemTablePositions() map[*hashmem.Memtable]int {
	return i.memTablePositions
}
func (i *PrefixIterator) incIndexByMem(mem *hashmem.Memtable) {
	i.memTablePositions[mem] += 1
}

func (i *PrefixIterator) IncrementMemTablePosition(memTablePtr *hashmem.Memtable) {
	a := *memTablePtr
	if a.GetMaxSize() == i.memTablePositions[memTablePtr] {
		i.memTablePositions[memTablePtr] = a.GetMaxSize()
	} else {
		i.incIndexByMem(memTablePtr)
	}

}
