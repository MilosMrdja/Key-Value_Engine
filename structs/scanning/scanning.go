package scanning

import (
	"sstable/cursor"
	"sstable/iterator"
	"sstable/mem/memtable/datatype"
)

// Function to perform PREFIX_SCAN
func PREFIX_SCAN(prefix string, pageNumber int, memIterator *iterator.PrefixIterator, ssIterator *iterator.IteratorPrefixSSTable, pageSize int, compress1 bool, compress2 bool, oneFile bool, cursor *cursor.Cursor) []*datatype.DataType {
	m := pageSize * (pageNumber - 1)
	n := pageSize
	page := make([]*datatype.DataType, 0)
	for m != 0 {
		PREFIX_ITERATE(prefix, memIterator, ssIterator, compress1, compress2, oneFile)
		m--
	}
	for n != 0 {
		a := PREFIX_ITERATE(prefix, memIterator, ssIterator, compress1, compress2, oneFile)
		page = append(page, &a)
		n--
	}
	return page
}

func RANGE_SCAN(valRange [2]string, pageNumber int, memIterator *iterator.RangeIterator, ssIterator *iterator.IteratorRangeSSTable, pageSize int, compress1 bool, compress2 bool, oneFile bool, cursor *cursor.Cursor) []*datatype.DataType {
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
