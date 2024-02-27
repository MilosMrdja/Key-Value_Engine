package scanning

import (
	"fmt"
	"sstable/iterator"
	"sstable/mem/memtable/datatype"
)

// Function to perform PREFIX_SCAN
func PREFIX_SCAN_OUTPUT(prefix string, pageNumber int, pageSize int, memIterator *iterator.PrefixIterator, ssIterator *iterator.IteratorPrefixSSTable, compress1 bool, compress2 bool, oneFile bool) {
	page := PREFIX_SCAN(prefix, pageNumber, pageSize, memIterator, ssIterator, compress1, compress2, oneFile)
	if len(page) == 0 {
		fmt.Printf("Ova stranica nema podataka.\n")
		return
	}
	for _, d := range page {
		fmt.Printf("Key: %s, Value: %s Time: %s\n", d.GetKey(), d.GetData(), d.GetChangeTime())
	}
}

func RANGE_SCAN_OUTPUT(valrange [2]string, pageNumber int, pageSize int, memIterator *iterator.RangeIterator, ssIterator *iterator.IteratorRangeSSTable, compress1 bool, compress2 bool, oneFile bool) {
	page := RANGE_SCAN(valrange, pageNumber, pageSize, memIterator, ssIterator, compress1, compress2, oneFile)
	if len(page) == 0 {
		fmt.Printf("Ova stranica nema podataka.\n")
		return
	}
	for _, d := range page {
		fmt.Printf("Key: %s, Value: %s Time: %s\n", d.GetKey(), d.GetData(), d.GetChangeTime())

	}
}
func PREFIX_SCAN(prefix string, pageNumber int, pageSize int, memIterator *iterator.PrefixIterator, ssIterator *iterator.IteratorPrefixSSTable, compress1 bool, compress2 bool, oneFile bool) []datatype.DataType {
	m := pageSize * (pageNumber - 1)
	n := pageSize
	page := make([]datatype.DataType, 0)
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
		page = append(page, a)
		n--
	}
	return page
}

func RANGE_SCAN(valRange [2]string, pageNumber int, pageSize int, memIterator *iterator.RangeIterator, ssIterator *iterator.IteratorRangeSSTable, compress1 bool, compress2 bool, oneFile bool) []datatype.DataType {
	m := pageSize * (pageNumber - 1)
	n := pageSize
	page := make([]datatype.DataType, 0)

	for m != 0 {
		_, flag := RANGE_ITERATE(valRange, memIterator, ssIterator, compress1, compress2, oneFile)
		if !flag {
			break
		}
		m--
	}
	for n != 0 {
		a, flag := RANGE_ITERATE(valRange, memIterator, ssIterator, compress1, compress2, oneFile)
		if !flag {
			break
		}
		page = append(page, a)
		n--
	}
	return page
	//for i := 0; i < n; i++ {
	//	for j := 0; j < pageSize; j++ {
	//		a, flag := RANGE_ITERATE(valRange, memIterator, ssIterator, compress1, compress2, oneFile)
	//		if !flag {
	//			break
	//		}
	//		page = append(page, a)
	//	}
	//
	//}

}
