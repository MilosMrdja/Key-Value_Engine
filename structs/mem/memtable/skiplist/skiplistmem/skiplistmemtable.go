package skiplistmem

import (
	"sstable/LSM"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/datatype"
	"sstable/mem/memtable/skiplist/skipliststruct"
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

// funkcija koja ce se implementirati kasnije a sluzi da prosledi podatke iz memtable u SSTable
// i da isprazni memtable kad se podaci posalju
func (slmem *SkipListMemtable) SendToSSTable(compress1, compress2, oneFile bool) bool {

	dataList := make([]datatype.DataType, slmem.length)
	dataList = slmem.data.AllData(slmem.length)

	newSstableName, _ := LSM.FindNextDestination(0)
	SSTable.NewSSTable(dataList, 1, 2, newSstableName, compress1, compress2, oneFile)
	SSTable.ReadSSTable(newSstableName, compress1, compress2, oneFile)

	slmem.data = skipliststruct.CreateSkipList(slmem.capacity)
	slmem.length = 0
	return true
}
func (slmem *SkipListMemtable) AddElement(key string, data []byte) bool {
	found, elem := slmem.data.GetElement(key)
	if found == false {
		//ukoliko ima mesta u memtable, samo se upisuje podatak
		if slmem.length < slmem.capacity {
			if slmem.data.Insert(key, data) == true {
				slmem.length++
				return true
			}
			return false

			//ako je popunjen, postavlja se na read only
		} else if slmem.length == slmem.capacity {
			slmem.readOnly = true
			return false
		}
	}
	elem.UpdateDataType(data)
	return true
}
func (slmem *SkipListMemtable) GetElement(key string) (bool, []byte) {
	err, elem := slmem.data.GetElement(key)
	if err == true {
		return true, elem.GetData()
	}
	return false, nil
}

func (slmem *SkipListMemtable) DeleteElement(key string) bool {
	if slmem.DeleteElement(key) == true {
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

func (slmem *SkipListMemtable) GetElementByPrefix(prefix string) []*datatype.DataType {
	return slmem.data.GetByPrefix(prefix)

}
