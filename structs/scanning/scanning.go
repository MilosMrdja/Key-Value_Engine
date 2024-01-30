package main

import (
	"fmt"
	"slices"
	"sort"
	"sstable/LSM"
	"sstable/cursor"
	"sstable/iterator"
	"sstable/mem/memtable/datatype"
	"strings"
)

func isInRange(value string, valRange []string) bool {
	return value >= valRange[0] && value <= valRange[1]
}

func PREFIX_ITERATE(prefix string, iterator *iterator.Iterator) {
	if prefix != iterator.CurrPrefix() {
		iterator.SetCurrPrefix(prefix)
		iterator.ResetMemTableIndexes()
	}

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
