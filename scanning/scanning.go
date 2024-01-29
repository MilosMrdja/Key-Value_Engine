package main

import (
	"fmt"
	"scanning/cursor"
	"scanning/mem/memtable/datatype"
	"slices"
	"sort"
	"strings"
)

// Function to perform PREFIX_SCAN
func PREFIX_SCAN(prefix string, pageNumber, pageSize int, cursor *cursor.Cursor) []*datatype.DataType {
	var result []*datatype.DataType
	var dt *datatype.DataType

	n := pageNumber * pageSize

	j := cursor.MemIndex()
	for true {
		lista := cursor.MemPointers()[j].GetElementByPrefix(prefix)
		for _, dt = range lista {
			if dt.IsDeleted() == false && slices.Contains(result, dt) == false {
				result = append(result, dt)
				n -= 1
				if n == 0 {
					break
				}
			}
		}
		j = (j - 1 + len(cursor.MemPointers())) % len(cursor.MemPointers())
		if j == cursor.MemIndex() {
			break
		}
		if n == 0 {
			break
		}
	}

	lruData := cursor.LruPointer().GetAll()
	for _, dt = range lruData {
		if slices.Contains(result, dt) == false && strings.HasPrefix(dt.GetKey(), prefix) && dt.IsDeleted() == false {
			result = append(result, dt)
			n -= 1
			if n == 0 {
				break
			}
		}
	}

	offset := 0
	path := ""

	for (len(result)) < n {
		ssData, offset, path = sstable.GetDataByPrefix(prefix, n, offset, path)
		for _, dt = range ssData {
			if slices.Contains(result, dt) == false && dt.IsDeleted() == false {
				result = append(result, dt)
				n -= 1
				if n == 0 {
					break
				}
			}
		}
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

func RANGE_SCAN(keyRange [2]string, pageNumber, pageSize int, cursor *cursor.Cursor) []*datatype.DataType {
	var result []*datatype.DataType
	var dt *datatype.DataType

	// Implement pagination
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize

	n := pageNumber * pageSize

	j := cursor.MemIndex()
	for true {
		lista := cursor.MemPointers()[j].GetElementByRange(keyRange)
		for _, dt = range lista {
			if dt.IsDeleted() == false && slices.Contains(result, dt) == false {
				result = append(result, dt)
				n -= 1
				if n == 0 {
					break
				}
			}
		}
		j = (j - 1 + len(cursor.MemPointers())) % len(cursor.MemPointers())
		if j == cursor.MemIndex() {
			break
		}
		if n == 0 {
			break
		}
	}

	lruData := cursor.LruPointer().GetAll()
	for _, dt = range lruData {
		if slices.Contains(result, dt) == false && strings.HasPrefix(dt.GetKey(), prefix) && dt.IsDeleted() == false {
			result = append(result, dt)
			n -= 1
			if n == 0 {
				break
			}
		}
	}

	offset := 0
	path := ""

	for (len(result)) < n {
		ssPodaci, offset, path = sstable.GetDataByRange(keyRange, n, offset, path)
		for _, dt = range ssPodaci {
			if slices.Contains(result, dt) == false && dt.IsDeleted() == false {
				result = append(result, dt)
				n -= 1
				if n == 0 {
					break
				}
			}
		}
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

	prefix := "abc"
	pageNumber := 1
	pageSize := 2

	result := PREFIX_SCAN(prefix, pageNumber, pageSize)

	// Display the result
	fmt.Printf("Results for prefix '%s', page %d, page size %d:\n", prefix, pageNumber, pageSize)
	for _, dt := range result {
		fmt.Printf("%s: %s\n", dt.Key, dt.Value, dt.Time)
	}
}
