package hashmem

import (
	"sstable/mem/memtable/datatype"
)

type Memtable interface {
	AddElement(key string, data []byte) bool
	GetElement(key string) (bool, []byte)
	DeleteElement(key string) bool
	SendToSSTable(compress1, compress2, oneFile bool, N, M int) bool
	IsReadOnly() bool
	GetElementByPrefix(prefix string) []*datatype.DataType
	GetElementByRange(valRange []string) []*datatype.DataType
}
