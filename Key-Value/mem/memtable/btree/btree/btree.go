package btree

import (
	"sstable/mem/memtable/btree/btreenode"
	"sstable/mem/memtable/btree/myutils"
	"sstable/mem/memtable/datatype"
	"time"
)

type BTree struct {
	Root *btreenode.BTreeNode
	Rang int
}

func InsertInplace(array []*btreenode.BTreeNode, i int, element *btreenode.BTreeNode) []*btreenode.BTreeNode {

	if array[i] == nil {
		array[i] = &btreenode.BTreeNode{}
	}
	array[i] = element
	return array
}
func (t *BTree) Delete(k string, time time.Time) bool {

	output, _ := t.Search(k)
	if output != nil {
		output.DeleteDataType(time)
		return true
	}

	return false
}
func (t *BTree) Insert(elem *datatype.DataType) bool {
	el, found := t.Search(elem.GetKey())
	if found {
		(*el) = *elem
		return false
	} else {
		el = el
		if t.Root == nil {
			t.Root = btreenode.NewBTreeNode(t.Rang, true)
			t.Root.SetKeys(myutils.InsertInplaceD(t.Root.Keys(), 0, elem))
			t.Root.SetN(1)
		} else {
			if t.Root.N() == 2*t.Rang-1 {
				s := btreenode.NewBTreeNode(t.Rang, false)
				s.SetChildren(InsertInplace(s.Children(), 0, t.Root))

				s.SplitChild(0, t.Root)

				i := 0
				if s.Keys()[0].GetKey() < elem.GetKey() {
					i++
				}
				s.Children()[i].InsertNonFull(elem)
				t.Root = s
			} else {
				t.Root.InsertNonFull(elem)
			}

		}
	}
	return true

}

func (t *BTree) Search(k string) (*datatype.DataType, bool) {
	if t.Root != nil {
		data := t.Root.Search(k)
		if data != nil {
			return data, true
		}
	}
	return nil, false
}

func (t *BTree) Update(k string, data []byte, time time.Time) bool {
	if t.Root != nil {
		e := t.Root.Search(k)
		e.UpdateDataType(data, time)
		return true
	}
	return false
}

func (t *BTree) Traverse() []datatype.DataType {
	dataList := make([]datatype.DataType, 0)
	if t.Root != nil {
		dataList = append(t.Root.Traverse())
	}
	return dataList
}

func (t *BTree) GetByPrefix(dataList []*datatype.DataType, n *int, prefix string) {

	if t.Root != nil {
		dataList = append(t.Root.GetByPrefix(n, prefix))
	}

}
func (t *BTree) GetByRange(dataList []*datatype.DataType, n *int, valRange []string) {

	if t.Root != nil {
		dataList = append(t.Root.GetByRange(n, valRange))
	}

}
func NewBTree(t int) *BTree {
	return &BTree{Root: nil, Rang: t}
}
