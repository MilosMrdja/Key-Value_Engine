package scanning

import (
	"sstable/SSTableStruct/SSTable"
	"sstable/iterator"
	"sstable/mem/memtable/datatype"
	"sstable/mem/memtable/hash/hashmem"
	"strings"
)

func RANGE_ITERATE(valueRange [2]string, memIterator *iterator.RangeIterator, ssIterator *iterator.IteratorRangeSSTable, compress1 bool, compress2 bool, oneFile bool) (datatype.DataType, bool) {
	if memIterator.ValRange()[0] != valueRange[0] || memIterator.ValRange()[1] != valueRange[1] {
		memIterator.SetValRange(valueRange)
		memIterator.ResetMemTableIndexes()
	}
	if valueRange[0] != ssIterator.Rang[0] || valueRange[1] != ssIterator.Rang[1] {
		ssIterator = RangeIterateSSTable(valueRange, compress1, compress2)
	}
	minMap := make(map[*hashmem.Memtable]datatype.DataType)
	for i := range memIterator.MemTablePositions() {
		for {
			j := *i
			if j.GetMaxSize() == memIterator.MemTablePositions()[i] {
				break

			} else if !isInRange(j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]].GetKey(), memIterator.ValRange()) && j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]].GetKey() > memIterator.ValRange()[1] {
				memIterator.MemTablePositions()[i] = j.GetMaxSize()
				break
			} else if isInRange(j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]].GetKey(), memIterator.ValRange()) {
				minMap[i] = j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]]
				break
			} else {
				memIterator.MemTablePositions()[i]++
			}
		}
	}
	minSsstableMap := make(map[string]datatype.DataType)
	for k, v := range ssIterator.PositionInSSTable {
		for {
			if ssIterator.PositionInSSTable[k][0] == ssIterator.PositionInSSTable[k][1] {
				break
			}
			record, offset := SSTable.GetRecord(k, ssIterator.PositionInSSTable[k][0], compress1, compress2)

			if !isInRange(record.GetKey(), ssIterator.Rang) && record.GetKey() > ssIterator.Rang[1] {
				v[0] = v[1]
				break
			} else if isInRange(record.GetKey(), ssIterator.Rang) {
				minSsstableMap[k] = record
				v[2] = uint64(offset)
				break
			} else {
				ssIterator.IncrementElementOffset(k, uint64(offset))
			}
		}
	}

	return AdjustPositionRange(minMap, minSsstableMap, ssIterator, memIterator)
}

// za memtabelu
func PREFIX_ITERATE(prefix string, memIterator *iterator.PrefixIterator, ssIterator *iterator.IteratorPrefixSSTable, compress1 bool, compress2 bool, oneFile bool) (datatype.DataType, bool) {
	if prefix != memIterator.CurrPrefix() {
		memIterator.SetCurrPrefix(prefix)
		memIterator.ResetMemTableIndexes()
	}
	if prefix != ssIterator.Prefix {
		ssIterator = PrefixIterateSSTable(prefix, compress1, compress2)
	}
	minMap := make(map[*hashmem.Memtable]datatype.DataType)
	for i := range memIterator.MemTablePositions() {
		for {
			j := *i
			if j.GetMaxSize() == memIterator.MemTablePositions()[i] {
				break
			} else if !strings.HasPrefix(j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]].GetKey(), memIterator.CurrPrefix()) && j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]].GetKey() > memIterator.CurrPrefix() {
				memIterator.MemTablePositions()[i] = j.GetMaxSize()
				break
			} else if strings.HasPrefix(j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]].GetKey(), memIterator.CurrPrefix()) {
				minMap[i] = j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]]
				break
			} else {
				memIterator.IncrementMemTablePosition(i)
			}
		}
	}
	minSsstableMap := make(map[string]datatype.DataType)
	for k, v := range ssIterator.GetSSTableMap() {
		for {
			if ssIterator.GetSSTableMap()[k][0] == ssIterator.GetSSTableMap()[k][1] {
				break
			}
			record, offset := SSTable.GetRecord(k, ssIterator.GetSSTableMap()[k][0], compress1, compress2)

			if !strings.HasPrefix(record.GetKey(), ssIterator.Prefix) && record.GetKey() > ssIterator.Prefix {
				v[0] = v[1]
				break
			} else if strings.HasPrefix(record.GetKey(), ssIterator.Prefix) {
				minSsstableMap[k] = record
				v[2] = uint64(offset)
				break
			} else {
				ssIterator.IncrementElementOffset(k, uint64(offset))
			}
		}
	}

	return AdjustPositionPrefix(minMap, minSsstableMap, ssIterator, memIterator)
}

/*
	l0	[125,65,200,269]  [125,150,200,269]
	l1  [0,0,0,0]  [1250,1500,2000,2690]
	l2  [0,0,0,0]  [12500,15000,20000,26900]

for SVE{
    l0  sst 1    aa
	l0 	sst2      ab
	L1 	SST1      aa

	L0  [[P],[K]]
	L1  [[P],[K]]
	L2  [[P],[K]]
}

lPocM, lKrajaM, elementM    aaj
lPoc, lKraja, element, bool		aab


if elementM < element{
		pokazi elemntM
}
*/

// za sstabelu
