package myutils

import (
	"sstable/mem/memtable/datatype"
)

func Insert[T any](array []T, i int, element T) []T {
	return append(array[:i], append([]T{element}, array[i:]...)...)
}

func InsertInplaceD(array []*datatype.DataType, i int, element *datatype.DataType) []*datatype.DataType {
	if array[i] == nil {
		array[i] = &datatype.DataType{}
	}
	array[i] = element
	return array

}
func Lenght(array []*datatype.DataType) int {
	k := 0
	for i := 0; i < len(array); i++ {
		if array[i] != nil {
			k++
		}

	}
	return k
}
