package skiplistmem

import (
	"sstable/LSM"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/datatype"
	"sstable/mem/memtable/skiplist/skipliststruct"
	"time"
)

type SkipListMemtable struct {
	data             *skipliststruct.SkipList
	capacity, length int
	readOnly         bool
}

func CreateSkipListMemtable(cap int) *SkipListMemtable {
	return &SkipListMemtable{
		data:     skipliststruct.CreateSkipList(cap),
		capacity: cap,
		length:   0,
		readOnly: false,
	}
}
func (slmem *SkipListMemtable) GetMaxSize() int {
	return slmem.length
}
func (slmem *SkipListMemtable) GetSortedDataTypes() []datatype.DataType {
	dataList := slmem.SortDataTypes()
	return dataList
}
func (slmem *SkipListMemtable) SortDataTypes() []datatype.DataType {
	dataList := make([]datatype.DataType, slmem.length)
	dataList = slmem.data.AllData(slmem.length)
	return dataList
}

// funkcija koja ce se implementirati kasnije a sluzi da prosledi podatke iz memtable u SSTable
// i da isprazni memtable kad se podaci posalju
func (slmem *SkipListMemtable) SendToSSTable(compress1, compress2, oneFile bool, N, M, maxSSTlevel int, prob float64) bool {

	dataList := make([]datatype.DataType, slmem.length)
	dataList = slmem.data.AllData(slmem.length)

	newSstableName, _ := LSM.FindNextDestination(0, maxSSTlevel)
	SSTable.NewSSTable(dataList, prob, N, M, newSstableName, compress1, compress2, oneFile)
	//SSTable.ReadSSTable(newSstableName, compress1, compress2)

	slmem.data = skipliststruct.CreateSkipList(slmem.capacity)
	slmem.length = 0
	slmem.readOnly = false
	return true
}

func (slmem *SkipListMemtable) UpdateElement(key string, data []byte, time time.Time) {
	_, elem := slmem.data.GetElement(key)
	elem.UpdateDataType(data, time)
	elem.SetDelete(false)
}

func (slmem *SkipListMemtable) AddElement(key string, data []byte, time time.Time) bool {

	//ukoliko ima mesta u memtable, samo se upisuje podatak
	if slmem.length < slmem.capacity {
		temp := datatype.CreateDataType(key, data, time)
		if slmem.data.Insert(temp) == true {
			slmem.length++
		}

	}
	//ako je popunjen, postavlja se na read only
	if slmem.length == slmem.capacity {
		slmem.readOnly = true
	}
	if slmem.IsReadOnly() {
		return true
	}
	return false

}
func (slmem *SkipListMemtable) GetElement(key string) (bool, *datatype.DataType) {
	err, elem := slmem.data.GetElement(key)
	if err == true {
		return true, elem
	}
	return false, nil
}

func (slmem *SkipListMemtable) DeleteElement(key string, time time.Time) bool {
	if slmem.data.DeleteElement(key, time) == true {
		slmem.length--
		return true
	}
	return false
}

func (slmem *SkipListMemtable) ShowSkipList() {
	slmem.data.ShowSkipList()
}

func (slmem *SkipListMemtable) IsReadOnly() bool {
	return slmem.readOnly
}

func (slmem *SkipListMemtable) GetElementByPrefix(resultList []*datatype.DataType, n *int, prefix string) {
	slmem.data.GetByPrefix(resultList[:], n, prefix)

}

func (slmem *SkipListMemtable) GetElementByRange(resultList []*datatype.DataType, n *int, valRange []string) {
	slmem.data.GetByRange(resultList[:], n, valRange)
}
