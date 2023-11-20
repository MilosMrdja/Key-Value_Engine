package memtable

type SkipListMemtable struct {
	data             *SkipList
	capacity, length int
	readOnly         bool
}

func CreateSkipListMemtable(cap int) *SkipListMemtable {
	return &SkipListMemtable{
		data:     CreateSkipList(cap),
		capacity: cap,
		length:   0,
		readOnly: false,
	}
}

func (slmem *SkipListMemtable) AddElement(key string, data []byte) bool {
	//ukoliko ima mesta u memtable, samo se upisuje podatak
	if slmem.length < slmem.capacity {
		return slmem.data.Insert(key, data)

		//ako je popunjen, postavlja se na read only
	} else if slmem.length == slmem.capacity {
		slmem.readOnly = true
		return false
	}
	//ukoliko se nesto nije izvrsilo kako treba, vraca se false
	return false
}
func (slmem *SkipListMemtable) GetElement(key string) (bool, []byte) {
	err, elem := slmem.data.GetElement(key)
	if err == true {
		return true, elem.data
	}
	return false, nil
}

func (slmem *SkipListMemtable) DeleteElement(key string) bool {
	return slmem.DeleteElement(key)
}

func (slmem *SkipListMemtable) ShowSkipList() {
	slmem.data.ShowSkipList()
}
