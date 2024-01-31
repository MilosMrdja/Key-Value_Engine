package iterator

import "sstable/mem/memtable/hash/hashmem"

type RangeIterator struct {
	memTablePositions map[*hashmem.Memtable]int
	valRange          [2]string
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
func NewRangeIterator(memTablePositions map[*hashmem.Memtable]int, valRange [2]string) *RangeIterator {
	return &RangeIterator{memTablePositions: memTablePositions, valRange: valRange}
}

// ===============================================================================
type PrefixIterator struct {
	memTablePositions map[*hashmem.Memtable]int
	currPrefix        string
}

func NewPrefixIterator(memTablePositions map[*hashmem.Memtable]int, currPrefix string) *PrefixIterator {
	return &PrefixIterator{memTablePositions: memTablePositions, currPrefix: currPrefix}
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
