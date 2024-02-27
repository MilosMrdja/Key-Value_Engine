package btreemem

import (
	"sstable/LSM"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/btree/btree"
	"sstable/mem/memtable/datatype"
	"time"
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

func (btmem *BTreeMemtable) UpdateElement(key string, data []byte, time time.Time) {
	btmem.data.Update(key, data, time)
	elem, _ := btmem.data.Search(key)
	elem.SetDelete(false)

}

func (btmem *BTreeMemtable) AddElement(key string, data []byte, time time.Time) bool {

	//ukoliko ima mesta u memtable, samo se upisuje podatak

	if btmem.length < btmem.capacity {
		e := datatype.CreateDataType(key, data, time)
		if btmem.data.Insert(e) {
			btmem.length++
		}

		//ako je popunjen, postavlja se na read only
	}
	if btmem.length == btmem.capacity {
		btmem.readOnly = true
	}
	if btmem.IsReadOnly() {
		return true
	}
	return false
}

func (btmem *BTreeMemtable) GetElement(key string) (bool, *datatype.DataType) {
	elem, err := btmem.data.Search(key)
	if !err {
		return false, nil
	}
	return true, elem
}
func (btmem *BTreeMemtable) GetMaxSize() int {
	return btmem.length
}
func (btmem *BTreeMemtable) DeleteElement(key string, time time.Time) bool {
	found := btmem.data.Delete(key, time)
	return found
}
func (btmem *BTreeMemtable) GetSortedDataTypes() []datatype.DataType {
	dataList := btmem.SortDataTypes()
	return dataList
}
func (btmem *BTreeMemtable) SortDataTypes() []datatype.DataType {
	dataList := make([]datatype.DataType, btmem.length)
	dataList = btmem.data.Traverse()
	return dataList
}

func (btmem *BTreeMemtable) SendToSSTable(compress1, compress2, oneFile bool, N, M, maxSSTlevel int, prob float64) bool {

	dataList := btmem.SortDataTypes()

	newSstableName, _ := LSM.FindNextDestination(0, maxSSTlevel)
	SSTable.NewSSTable(dataList, prob, N, M, newSstableName, compress1, compress2, oneFile)
	//SSTable.ReadSSTable(newSstableName, compress1, compress2)

	btmem.data = btree.NewBTree(btmem.capacity)
	btmem.length = 0
	btmem.readOnly = false

	return true
}

func (btmem *BTreeMemtable) IsReadOnly() bool {
	return btmem.ReadOnly()
}

func (btmem *BTreeMemtable) GetElementByPrefix(dataList []*datatype.DataType, n *int, prefix string) {
	btmem.data.GetByPrefix(dataList[:], n, prefix)
}
func (btmem *BTreeMemtable) GetElementByRange(dataList []*datatype.DataType, n *int, valRange []string) {
	btmem.data.GetByRange(dataList[:], n, valRange)
}
