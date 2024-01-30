package hashmem

import (
	"sstable/mem/memtable/datatype"
)

type Memtable interface {
	AddElement(key string, data []byte) bool
	GetElement(key string) (bool, []byte)
	DeleteElement(key string) bool
	SortDataTypes() []datatype.DataType
	GetMaxSize() int
	SendToSSTable(compress1, compress2, oneFile bool, N, M int) bool
	IsReadOnly() bool
	GetElementByPrefix(resultList []*datatype.DataType, n *int, prefix string)
	GetElementByRange(resultList []*datatype.DataType, n *int, valRange []string)
}
