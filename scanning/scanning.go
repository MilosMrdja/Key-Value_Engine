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

	// Implement pagination
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize

	// iterate through resources and find elements
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

	// Return the paginated result
	return result[startIndex:endIndex]
}

func main() {

	prefix := "abc"
	pageNumber := 1
	pageSize := 2

	range := [2]int{10, 50}

	prefix_result := PREFIX_SCAN(prefix, pageNumber, pageSize)
	range_result := RANGE_SCAN(range, pageNumber, pageSize)

	// Display the result of PREFIX_SCAN
	fmt.Printf("Results for prefix '%s', page %d, page size %d:\n", prefix, pageNumber, pageSize)
	for _, dt := range prefix_result {
		fmt.Printf("%s: %s\n", dt.Key, dt.Value, dt.Time)
	}

	// Display the result of RANGE_SCAN
	fmt.Printf("Results for range ['%d', '%d'], page %d, page size %d:\n", range[0], range[1], pageNumber, pageSize)
	for _, dt := range range_result {
		fmt.Printf("%s: %s\n", dt.Key, dt.Value, dt.Time)
	}
}
