package memtable

type HashMemtable struct {
	data             map[string]*DataType
	capacity, length int
	readOnly         bool
}

func CreateHashMemtable(cap int) *HashMemtable {
	return &HashMemtable{
		data:     make(map[string]*DataType),
		capacity: cap,
		length:   0,
		readOnly: false,
	}
}

// funkcija koja ce se implementirati kasnije a sluzi da prosledi podatke iz memtable u SSTable
// i da isprazni memtable kad se podaci posalju
func (mem *HashMemtable) SendToSSTable() bool {

	//.......
	//.......
	mem.data = make(map[string]*DataType)
	mem.length = 0
	return true
}

func (mem *HashMemtable) AddElement(key string, data []byte) bool {
	//ukoliko ima mesta u memtable, samo se upisuje podatak
	if mem.length < mem.capacity {
		e := CreateDataType(data)
		mem.data[key] = e
		mem.length++
		return true

		//ako je popunjen, postavlja se na read only
	} else if mem.length == mem.capacity {
		mem.readOnly = true
		return false
	}
	//ukoliko se nesto nije izvrsilo kako treba, vraca se false
	return false
}

func (mem *HashMemtable) GetElement(key string) (bool, []byte) {
	elem, err := mem.data[key]
	if !err || elem.IsDeleted() {
		return false, nil
	}
	return true, elem.data
}

func (mem *HashMemtable) DeleteElement(key string) bool {
	elem, found := mem.data[key]
	if found {
		elem.DeleteDataType()
		return true
	}
	return false
}
