package main

import (
	"fmt"
	"sort"
	"strings"
)

// Function to perform PREFIX_SCAN
func PREFIX_SCAN(prefix string, pageNumber, pageSize int) []DataType {
	var result []DataType

	i := 0

	// Implement pagination
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize

	// mem, hes, sstable, tim redom
	for key := range table {
		if i >= startIndex {
			if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
				result = append(result, DataType{Key: key, Value: table[key]})
			}
		}
		i += 1
		if i == endIndex {
			break
		}
	}

	// Sort the result by key
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	// Return the paginated result
	return result[startIndex:endIndex]
}

func RANGE_SCAN(keyRange [2]int, pageNumber, pageSize int) []DataType {
	var result []DataType

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
