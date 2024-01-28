package btreemem

import (
	"sstable/LSM"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/btree/btree"
	"sstable/mem/memtable/datatype"
)

type BTreeMemtable struct {
	data             *btree.BTree
	capacity, length int
	readOnly         bool
}

func (B *BTreeMemtable) Data() *btree.BTree {
	return B.data
}

func (B *BTreeMemtable) SetData(data *btree.BTree) {
	B.data = data
}

func (B *BTreeMemtable) Capacity() int {
	return B.capacity
}

func (B *BTreeMemtable) SetCapacity(capacity int) {
	B.capacity = capacity
}

func (B *BTreeMemtable) Length() int {
	return B.length
}

func (B *BTreeMemtable) SetLength(length int) {
	B.length = length
}

func (B *BTreeMemtable) ReadOnly() bool {
	return B.readOnly
}

func (B *BTreeMemtable) SetReadOnly(readOnly bool) {
	B.readOnly = readOnly
}

func NewBTreeMemtable(capacity int) *BTreeMemtable {
	return &BTreeMemtable{
		data:     btree.NewBTree(4),
		capacity: capacity,
		length:   0,
		readOnly: false,
	}
}

func (btmem *BTreeMemtable) AddElement(key string, data []byte) bool {

	//provera da li element sa tim kljucem vec postoji
	_, found := btmem.data.Search(key)
	if found == false {
		//ukoliko ima mesta u memtable, samo se upisuje podatak
		if btmem.length < btmem.capacity {
			e := datatype.CreateDataType(key, data)
			btmem.data.Insert(e)
			btmem.length++
			return true

			//ako je popunjen, postavlja se na read only
		} else if btmem.length == btmem.capacity {
			btmem.readOnly = true
			return false
		}
	}
	// ukoliko podatak sa tim kljucem postoji azuriramo podatak
	btmem.data.Update(key, data)
	return true
}

func (btmem *BTreeMemtable) GetElement(key string) (bool, []byte) {
	elem, err := btmem.data.Search(key)
	if !err || elem.IsDeleted() {
		return false, nil
	}
	return true, elem.GetData()
}

func (btmem *BTreeMemtable) DeleteElement(key string) bool {
	found := btmem.data.Delete(key)
	return found
}

func (btmem *BTreeMemtable) SendToSSTable(compress1, compress2, oneFile bool) bool {
	dataList := make([]datatype.DataType, btmem.length)
	dataList = btmem.data.Traverse()

	newSstableName, _ := LSM.FindNextDestination(0)
	SSTable.NewSSTable(dataList, 1, 2, newSstableName, compress1, compress2, oneFile)
	SSTable.ReadSSTable(newSstableName, compress1, compress2, oneFile)
	btmem.data = btree.NewBTree(btmem.capacity)
	btmem.length = 0

	return true
}

func (btmem *BTreeMemtable) IsReadOnly() bool {
	return btmem.ReadOnly()
}

func (btmem *BTreeMemtable) GetElementByPrefix(prefix string) []*datatype.DataType {
	return btmem.data.GetByPrefix(prefix)
}
