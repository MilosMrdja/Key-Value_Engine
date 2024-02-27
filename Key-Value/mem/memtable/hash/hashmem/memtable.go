package hashmem

import (
	"sstable/mem/memtable/datatype"
	"time"
)

type Memtable interface {
	/*
		ukoliko se posle dodavanja novog zapisa popuni memtable vraca se true
		u suprotnom se vraca false
	*/
	AddElement(key string, data []byte, time time.Time) bool
	UpdateElement(key string, data []byte, time time.Time)
	GetElement(key string) (bool, *datatype.DataType)
	DeleteElement(key string, time time.Time) bool
	SortDataTypes() []datatype.DataType
	GetSortedDataTypes() []datatype.DataType
	GetMaxSize() int
	/*
		Zapisi iz memtable se sortiraju i upisuju na disk
		Potom se memtable "prazni" postavljanjem broja elemenata na 0, kreiranjem prazne strukture i postavljanjem ReadOnly na false
	*/
	SendToSSTable(compress1, compress2, oneFile bool, N, M, maxSSTlevel int, prob float64) bool
	IsReadOnly() bool
	GetElementByPrefix(resultList []*datatype.DataType, n *int, prefix string)
	GetElementByRange(resultList []*datatype.DataType, n *int, valRange []string)
}
