package hashstruct

import (
	"sort"
	"sstable/mem/memtable/datatype"
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

// funkcija koja ce se implementirati kasnije a sluzi da prosledi podatke iz memtable u SSTable
// i da isprazni memtable kad se podaci posalju
func (mem *HashMemtable) SendToSSTable(compres bool) bool {

	dataList := make([]datatype.DataType, mem.length)
	i := 0
	for _, data := range mem.data {
		dataList[i] = *data
		i++
	}
	sort.Slice(dataList, func(i, j int) bool {
		return dataList[i].GetKey() < dataList[j].GetKey()
	})

	//napravimo SSTable
	//...
	//...

	mem.data = make(map[string]*datatype.DataType)
	mem.length = 0
	return true
}

func (mem *HashMemtable) AddElement(key string, data []byte) bool {
	//provera da li element sa tim kljucem vec postoji
	elem, _ := mem.GetElement(key)
	if elem == false {
		//ukoliko ima mesta u memtable, samo se upisuje podatak
		if mem.length < mem.capacity {
			e := datatype.CreateDataType(key, data)
			mem.data[key] = e
			mem.length++
			return true

			//ako je popunjen, postavlja se na read only
		} else if mem.length == mem.capacity {
			mem.readOnly = true
			return false
		}
	}
	// ukoliko podatak sa tim kljucem postoji azuriramo podatak
	mem.data[key].UpdateDataType(data)
	return true
}

func (mem *HashMemtable) GetElement(key string) (bool, []byte) {
	elem, err := mem.data[key]
	if !err || elem.IsDeleted() {
		return false, nil
	}
	return true, elem.GetData()
}

func (mem *HashMemtable) DeleteElement(key string) bool {
	elem, found := mem.data[key]
	if found {
		elem.DeleteDataType()
		return true
	}
	return false
}

func (mem *HashMemtable) IsReadOnly() bool {
	return mem.readOnly
}
