package iterator

import "sstable/mem/memtable/hash/hashmem"

type Iterator struct {
	memTablePositions map[*hashmem.Memtable]int
	currPrefix        string
}

func (i *Iterator) ResetMemTableIndexes() {
	for k := range i.memTablePositions {
		i.memTablePositions[k] = 0
	}
}

func (i *Iterator) CurrPrefix() string {
	return i.currPrefix
}

func (i *Iterator) SetCurrPrefix(currPrefix string) {
	i.currPrefix = currPrefix
}

func (i *Iterator) MemTablePositions() map[*hashmem.Memtable]int {
	return i.memTablePositions
}

func (i *Iterator) IncrementMemTablePosition(memTablePtr hashmem.Memtable) {
	if memTablePtr.GetMaxSize() == i.memTablePositions[&memTablePtr] {
		i.memTablePositions[&memTablePtr] = memTablePtr.GetMaxSize()
	} else {
		i.memTablePositions[&memTablePtr]++
	}

}
