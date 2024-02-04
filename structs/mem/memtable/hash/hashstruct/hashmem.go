package hashstruct

import (
	"sort"
	"sstable/LSM"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/datatype"
	"strings"
	"time"
)

type HashMemtable struct {
	data             map[string]*datatype.DataType
	capacity, length int
	readOnly         bool
}

func CreateHashMemtable(cap int) *HashMemtable {
	return &HashMemtable{
		data:     make(map[string]*datatype.DataType),
		capacity: cap,
		length:   0,
		readOnly: false,
	}
}
func isInRange(value string, valRange []string) bool {
	return value >= valRange[0] && value <= valRange[1]
}
func (mem *HashMemtable) GetMaxSize() int {
	return mem.length
}
func (mem *HashMemtable) GetSortedDataTypes() []datatype.DataType {
	dataList := mem.SortDataTypes()
	return dataList
}

// funkcija koja ce se implementirati kasnije a sluzi da prosledi podatke iz memtable u SSTable
// i da isprazni memtable kad se podaci posalju
func (mem *HashMemtable) SortDataTypes() []datatype.DataType {
	dataList := make([]datatype.DataType, mem.length)
	i := 0
	for _, data := range mem.data {
		dataList[i] = *data
		i++
	}
	sort.Slice(dataList, func(i, j int) bool {
		return dataList[i].GetKey() < dataList[j].GetKey()
	})
	return dataList

}

func (mem *HashMemtable) SendToSSTable(compress1, compress2, oneFile bool, N, M, maxSSTlevel int, prob float64) bool {

	dataList := mem.SortDataTypes()
	newSstableName, _ := LSM.FindNextDestination(0, maxSSTlevel)
	SSTable.NewSSTable(dataList, prob, N, M, newSstableName, compress1, compress2, oneFile)
	//SSTable.ReadSSTable(newSstableName, compress1, compress2)

	mem.data = make(map[string]*datatype.DataType)
	mem.length = 0
	mem.readOnly = false
	return true
}

func (mem *HashMemtable) UpdateElement(key string, data []byte, time time.Time) {
	mem.data[key].UpdateDataType(data, time)
	mem.data[key].SetDelete(false)
}

func (mem *HashMemtable) AddElement(key string, data []byte, time time.Time) bool {
	//ukoliko ima mesta u memtable, samo se upisuje podatak
	if mem.length < mem.capacity {
		e := datatype.CreateDataType(key, data, time)
		mem.data[key] = e
		mem.length++

		//ako je popunjen, postavlja se na read only
	}
	if mem.length == mem.capacity {
		mem.readOnly = true
	}
	if mem.IsReadOnly() {
		return true
	}
	return false
}

func (mem *HashMemtable) GetElement(key string) (bool, *datatype.DataType) {
	elem, err := mem.data[key]
	if !err {
		return false, nil
	}
	return true, elem
}

func (mem *HashMemtable) DeleteElement(key string, time time.Time) bool {
	elem, found := mem.data[key]
	if found {
		elem.DeleteDataType(time)
		return true
	}
	return false
}

func (mem *HashMemtable) IsReadOnly() bool {
	return mem.readOnly
}

func (mem *HashMemtable) GetElementByPrefix(dataList []*datatype.DataType, n *int, prefix string) {

	//for key, value := range mem.data {
	//	if strings.HasPrefix(key, prefix) && !value.IsDeleted() {
	//		if *n == 0 {
	//			return
	//		}
	//		dataList = append(dataList, value)
	//		*n--
	//	}
	//}
	for key, value := range mem.data {
		if strings.HasPrefix(key, prefix) && !value.IsDeleted() {
			if *n == 0 {
				return
			}
			dataList = append(dataList, value)
			*n--
		}
	}

}
func (mem *HashMemtable) GetElementByRange(dataList []*datatype.DataType, n *int, valRange []string) {

	for key, value := range mem.data {

		if isInRange(key, valRange) && !value.IsDeleted() {
			if *n == 0 {
				return
			}
			dataList = append(dataList, value)
			*n--
		}
	}

}
