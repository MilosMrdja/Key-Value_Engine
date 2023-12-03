package btree

import (
	"awesomeProject/btreenode"
	"awesomeProject/btreenode/datatype"
	"awesomeProject/myutils"
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

func (t *BTree) Insert(k string) {
	if t.Root == nil {
		t.Root = btreenode.NewBTreeNode(t.Rang, true)
		t.Root.SetKeys(myutils.InsertInplaceD(t.Root.Keys(), 0, datatype.CreateDataType(k, make([]byte, 2))))
		t.Root.SetN(1)
	} else {
		if t.Root.N() == 2*t.Rang-1 {
			s := btreenode.NewBTreeNode(t.Rang, false)
			s.SetChildren(InsertInplace(s.Children(), 0, t.Root))

			s.SplitChild(0, t.Root)

			i := 0
			if s.Keys()[0].GetKey() < k {
				i++
			}
			s.Children()[i].InsertNonFull(k)
			t.Root = s
		} else {
			t.Root.InsertNonFull(k)
		}

	}
}

func (t *BTree) Search(k string) *datatype.DataType {
	if t.Root != nil {
		return t.Root.Search(k)
	}
	return nil
}

func (t *BTree) Delete(k string) bool {

	output := t.Search(k)
	if output != nil {
		output.DeleteDataType()
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
