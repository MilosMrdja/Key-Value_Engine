package scanning

import (
	"fmt"
	"sstable/iterator"
	"sstable/mem/memtable/datatype"
)

// Function to perform PREFIX_SCAN
func PREFIX_SCAN_OUTPUT(prefix string, pageNumber int, pageSize int, memIterator *iterator.PrefixIterator, ssIterator *iterator.IteratorPrefixSSTable, compress1 bool, compress2 bool, oneFile bool) {
	page := PREFIX_SCAN(prefix, pageNumber, pageSize, memIterator, ssIterator, compress1, compress2, oneFile)
	for _, d := range page {
		fmt.Printf("Key: %s, Value: %s", d.GetKey(), d.GetData())
	}
}
func RANGE_SCAN_OUTPUT(valrange [2]string, pageNumber int, pageSize int, memIterator *iterator.RangeIterator, ssIterator *iterator.IteratorRangeSSTable, compress1 bool, compress2 bool, oneFile bool) {
	page := RANGE_SCAN(valrange, pageNumber, pageSize, memIterator, ssIterator, compress1, compress2, oneFile)
	for _, d := range page {
		fmt.Printf("Key: %s, Value: %s", d.GetKey(), d.GetData())
	}
}
func PREFIX_SCAN(prefix string, pageNumber int, pageSize int, memIterator *iterator.PrefixIterator, ssIterator *iterator.IteratorPrefixSSTable, compress1 bool, compress2 bool, oneFile bool) []*datatype.DataType {
	m := pageSize * (pageNumber - 1)
	n := pageSize
	page := make([]*datatype.DataType, 0)
	for m != 0 {
		_, flag := PREFIX_ITERATE(prefix, memIterator, ssIterator, compress1, compress2, oneFile)
		if !flag {
			break
		}
		m--
	}
	for n != 0 {
		a, flag := PREFIX_ITERATE(prefix, memIterator, ssIterator, compress1, compress2, oneFile)
		if !flag {
			break
		}
		page = append(page, &a)
		n--
	}
	return page
}

func RANGE_SCAN(valRange [2]string, pageNumber int, pageSize int, memIterator *iterator.RangeIterator, ssIterator *iterator.IteratorRangeSSTable, compress1 bool, compress2 bool, oneFile bool) []*datatype.DataType {
	m := pageSize * (pageNumber - 1)
	n := pageSize
	page := make([]*datatype.DataType, 0)
	for m != 0 {
		RANGE_ITERATE(valRange, memIterator, ssIterator, compress1, compress2, oneFile)
		m--
	}
	for n != 0 {
		a := RANGE_ITERATE(valRange, memIterator, ssIterator, compress1, compress2, oneFile)
		page = append(page, &a)
		n--
	}
	return page
}
