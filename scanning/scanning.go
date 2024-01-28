package main

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// Function to perform PREFIX_SCAN
func PREFIX_SCAN(prefix string, pageNumber, pageSize int, cursor *Cursor) []*DataType {
	var result []*DataType

	i := 0

	// Implement pagination
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize

	n := pageNumber * pageSize

	j := cursor.memIndex
	for true {
		lista := cursor.memPointers(j).GetDataByPrefix(prefix)
		for dt := range lista {
			if dt.delete == true || slices.Contains(result, dt) {
				result = append(result, dt)
				n -= 1
				if n == 0 {
					break
				}
			}
		}
		j = (j - 1 + len(cursor.memPointers)) % len(cursor.memPointers)
		if j == cursor.memIndex {
			break
		}
		if n == 0 {
			break
		}
	}

	lruData := cursor.lruPointer.getAll()
	for dt := range lruData {
		if slices.Contains(result, dt) {
			result = append(result, dt)
			n -= 1
			if n == 0 {
				break
			}
		}
	}

	offset := 0
	path := ""

	for (len(memPodaci) + len(kesPodaci) + len(ssPodaci)) < n {
		ssPodaci, offset, path = sstable.CitajPodateke(prefix, n-len(memPodaci)-len(kesPodaci), offset, path)
		for dt := range ssPodaci {
			if slices.Contains(result, dt) {
				result = append(result, dt)
				n -= 1
				if n == 0 {
					break
				}
			}
		}
	}

	// Sort the result by key
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	// Return the paginated result
	return result[startIndex:endIndex]
}

func RANGE_SCAN(keyRange [2]string, pageNumber, pageSize int, cursor *Cursor) []*DataType {
	var result []*DataType

	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize

	for key := range table {
		if strings.Compare(key, keyRange) >= 0 {
			result = append(result, DataType{Key: key, Value: value})
		}
	}

	// Sort the result by key
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	if endIndex > len(result) {
		endIndex = len(result)
	}
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
