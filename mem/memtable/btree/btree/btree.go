package btree

import (
	"mem/memtable/btree/btreenode"
	"mem/memtable/btree/myutils"
	"mem/memtable/datatype"
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
func (t *BTree) Delete(k string) bool {

	output, _ := t.Search(k)
	if output != nil {
		output.DeleteDataType()
		return true
	}

	return false
}
func (t *BTree) Insert(elem *datatype.DataType) {
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

func (t *BTree) Search(k string) (*datatype.DataType, bool) {
	if t.Root != nil {
		data := t.Root.Search(k)
		if data != nil {
			return data, true
		}
	}
	return nil, false
}

func (t *BTree) Update(k string, data []byte) bool {
	if t.Root != nil {
		e := t.Root.Search(k)
		e.UpdateDataType(data)
		return true
	}
	return false
}

func (t *BTree) Traverse() {
	if t.Root != nil {
		t.Root.Traverse()
	}
}

func NewBTree(t int) *BTree {
	return &BTree{Root: nil, Rang: t}
}
