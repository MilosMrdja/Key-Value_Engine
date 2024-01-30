package main

import (
	"fmt"
	"slices"
	"sort"
	"sstable/LSM"
	"sstable/cursor"
	"sstable/iterator"
	"sstable/mem/memtable/datatype"
	"sstable/mem/memtable/hash/hashmem"
	"strings"
)

func isInRange(value string, valRange []string) bool {
	return value >= valRange[0] && value <= valRange[1]
}

func minFind(arrPos []*hashmem.Memtable, arrValues []datatype.DataType) []*hashmem.Memtable {
	sort.Slice(arrValues[:], func(i, j int) bool {
		return arrValues[i].GetChangeTime().After(arrValues[j].GetChangeTime())
	})
	minArray := make([]*hashmem.Memtable, 0)
	minArray = append(minArray, arrPos[0])
	for i := 1; i < len(arrValues); i++ {
		if arrValues[i].GetKey() > arrValues[i-1].GetKey() {
			break
		} else {
			minArray = append(minArray, arrPos[i])
		}
	}
	return minArray
}
func adjustPositions(mapa map[*hashmem.Memtable]datatype.DataType, iterator *iterator.Iterator) datatype.DataType {
	keys := make([]*hashmem.Memtable, 0, len(mapa))
	values := make([]datatype.DataType, 0, len(mapa))

	for key := range mapa {
		keys = append(keys, key)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		a := mapa[keys[i]]
		b := mapa[keys[j]]
		return a.GetKey() < b.GetKey()
	})

	for _, k := range keys {
		values = append(values, mapa[k])
	}
	minArray := minFind(keys, values)
	for _, k := range minArray {
		iterator.IncrementMemTablePosition(*k)
	}
	return mapa[minArray[0]]
}
func PREFIX_ITERATE(prefix string, iterator *iterator.Iterator) {
	if prefix != iterator.CurrPrefix() {
		iterator.SetCurrPrefix(prefix)
		iterator.ResetMemTableIndexes()
	}
	minMap := make(map[*hashmem.Memtable]datatype.DataType)
	for i := range iterator.MemTablePositions() {
		for {
			if i.GetMaxSize() == iterator.MemTablePositions()[i] {
				break
			} else if !strings.HasPrefix(i.GetSortedDataTypes()[iterator.MemTablePositions()[i]].GetKey(), iterator.CurrPrefix()) && i.GetSortedDataTypes()[iterator.MemTablePositions()[i]].GetKey() > iterator.CurrPrefix() {
				iterator.MemTablePositions()[i] = i.GetMaxSize()
				break
			} else if strings.HasPrefix(i.GetSortedDataTypes()[iterator.MemTablePositions()[i]].GetKey(), iterator.CurrPrefix()) {
				minMap[&i] = i.GetSortedDataTypes()[iterator.MemTablePositions()[i]]
				break
			} else {
				iterator.MemTablePositions()[i]++
			}
		}
	}
	minInMems := adjustPositions(minMap, iterator)
	fmt.Println(minInMems)
}

// Function to perform PREFIX_SCAN
func PREFIX_SCAN(prefix string, pageNumber, pageSize int, cursor *cursor.Cursor) []*datatype.DataType {
	var result []*datatype.DataType
	var dt *datatype.DataType

	n := pageNumber * pageSize

	j := cursor.MemIndex()

	for true {
		cursor.MemPointers()[j].GetElementByPrefix(result, &n, prefix)

		j = (j - 1 + len(cursor.MemPointers())) % len(cursor.MemPointers())
		if j == cursor.MemIndex() {
			break
		}
		if n == 0 {
			break
		}
	}

	lruData := cursor.LruPointer().GetAll()
	for e := lruData.Front(); e != nil; e = e.Next() {
		dt = e.Value.(*datatype.DataType)
		if slices.Contains(result, dt) == false && strings.HasPrefix(dt.GetKey(), prefix) && dt.IsDeleted() == false {
			result = append(result, dt)
			n -= 1
			if n == 0 {
				break
			}
		}
	}

	for (len(result)) < n {
		ssData, _, _, _ := LSM.GetDataByPrefix(&n, prefix, cursor.Compress1(), cursor.Compress2(), cursor.OneFile())
		fmt.Println(ssData)
		if n == 0 {
			break
		}
	}

	// Implement pagination
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize

	// Sort the result by key
	sort.Slice(result, func(i, j int) bool {
		return result[i].GetKey() < result[j].GetKey()
	})

	// Return the paginated result
	return result[startIndex:endIndex]
}

func RANGE_SCAN(keyRange []string, pageNumber, pageSize int, core *cursor.Cursor) []*datatype.DataType {
	var result []*datatype.DataType
	var dt *datatype.DataType

	// Implement pagination
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize

	n := pageNumber * pageSize

	j := core.MemIndex()
	for true {
		core.MemPointers()[j].GetElementByRange(result, &n, keyRange)

		j = (j - 1 + len(core.MemPointers())) % len(core.MemPointers())
		if j == core.MemIndex() {
			break
		}
		if n == 0 {
			break
		}
	}
	lruData := core.LruPointer().GetAll()
	for e := lruData.Front(); e != nil; e = e.Next() {
		dt = e.Value.(*datatype.DataType)
		if isInRange(dt.GetKey(), keyRange) && dt.IsDeleted() == false {
			result = append(result, dt)
			n -= 1
			if n == 0 {
				break
			}
		}
	}

	//offset := 0
	//path := ""

	for (len(result)) < n {
		ssData, _, _, _ := LSM.GetDataByRange(&n, keyRange, core.Compress1(), core.Compress2(), core.OneFile())
		fmt.Println(ssData)
		if n == 0 {
			break
		}
	}

	// Sort the result by key
	sort.Slice(result, func(i, j int) bool {
		return result[i].GetKey() < result[j].GetKey()
	})

	// Return the paginated result
	return result[startIndex:endIndex]
}

func main() {

	fmt.Println("Main")
}
