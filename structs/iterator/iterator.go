package iterator

import "sstable/mem/memtable/hash/hashmem"

type RangeIterator struct {
	memTablePositions map[hashmem.Memtable]int
	valRange          []string
}

func (i *RangeIterator) ResetMemTableIndexes() {
	for k := range i.memTablePositions {
		i.memTablePositions[k] = 0
	}
}

func (i *RangeIterator) AllOnEnd() bool {
	j := 0
	for mem := range i.memTablePositions {
		if mem.GetMaxSize() == i.memTablePositions[mem] {
			j++
		}
	}
	if j == len(i.memTablePositions) {
		return true
	}
	return false
}

func (i *RangeIterator) ValRange() []string {
	return i.valRange
}

func (i *RangeIterator) SetValRange(valrange []string) {
	i.valRange = valrange
}

func (i *RangeIterator) MemTablePositions() map[hashmem.Memtable]int {
	return i.memTablePositions
}

func (i *RangeIterator) IncrementMemTablePosition(memTablePtr hashmem.Memtable) {
	if memTablePtr.GetMaxSize() == i.memTablePositions[memTablePtr] {
		i.memTablePositions[memTablePtr] = memTablePtr.GetMaxSize()
	} else {
		i.memTablePositions[memTablePtr]++
	}

}
func NewRangeIterator(memTablePositions map[hashmem.Memtable]int, valRange []string) *RangeIterator {
	return &RangeIterator{memTablePositions: memTablePositions, valRange: valRange}
}

// ===============================================================================
type PrefixIterator struct {
	memTablePositions map[hashmem.Memtable]int
	currPrefix        string
}

func (i *PrefixIterator) ResetMemTableIndexes() {
	for k := range i.memTablePositions {
		i.memTablePositions[k] = 0
	}
}

func (i *PrefixIterator) AllOnEnd() bool {
	j := 0
	for mem := range i.memTablePositions {
		if mem.GetMaxSize() == i.memTablePositions[mem] {
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

func (i *PrefixIterator) MemTablePositions() map[hashmem.Memtable]int {
	return i.memTablePositions
}

func (i *PrefixIterator) IncrementMemTablePosition(memTablePtr hashmem.Memtable) {
	if memTablePtr.GetMaxSize() == i.memTablePositions[memTablePtr] {
		i.memTablePositions[memTablePtr] = memTablePtr.GetMaxSize()
	} else {
		i.memTablePositions[memTablePtr]++
	}

}
