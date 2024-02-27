package btreenode

import (
	"sstable/mem/memtable/btree/myutils"
	"sstable/mem/memtable/datatype"
	"strings"
)

type BTreeNode struct {
	isLeaf   bool
	keys     []*datatype.DataType
	children []*BTreeNode
	n        int
	t        int
}

func isInRange(value string, valRange []string) bool {
	return value >= valRange[0] && value <= valRange[1]
}
func NewBTreeNode(t int, leaf bool) *BTreeNode {
	return &BTreeNode{
		isLeaf:   leaf,
		t:        t,
		keys:     make([]*datatype.DataType, 2*t-1),
		children: make([]*BTreeNode, 2*t),
		n:        0,
	}
}
func (b *BTreeNode) InsertNonFull(elem *datatype.DataType) {
	i := b.n - 1
	if b.isLeaf {

		for i >= 0 && b.keys[i].GetKey() > elem.GetKey() {
			b.keys[i+1] = b.keys[i]
			i--
		}
		b.SetKeys(myutils.InsertInplaceD(b.keys, i+1, elem))
		b.n++
	} else {
		for i >= 0 && b.keys[i].GetKey() > elem.GetKey() {
			i--
		}

		// See if the found child is full
		if b.children[i+1].n == 2*b.t-1 {
			b.SplitChild(i+1, b.children[i+1])
			if b.keys[i+1].GetKey() < elem.GetKey() {
				i++
			}

		}

		b.children[i+1].InsertNonFull(elem)

	}

}

func (b *BTreeNode) SplitChild(i int, y *BTreeNode) {
	z := NewBTreeNode(y.t, y.isLeaf)
	z.n = b.t - 1

	for j := 0; j < b.t-1; j++ {
		z.keys[j] = y.keys[j+b.t]
	}

	if y.isLeaf == false {
		for j := 0; j < b.t; j++ {
			z.children[j] = y.children[j+b.t]
		}
	}
	y.n = b.t - 1

	for j := b.n; j >= i+1; j-- {
		b.children[j+1] = b.children[j]
	}

	b.children[i+1] = z

	for j := b.n - 1; j >= i; j-- {
		b.keys[j+1] = b.keys[j]
	}

	b.keys[i] = y.keys[b.t-1]

	b.n = b.n + 1

}
func (b *BTreeNode) Search(k string) *datatype.DataType {
	var i = 0
	for i < b.n && k > b.keys[i].GetKey() {
		i++
	}

	if i < b.n && k == b.keys[i].GetKey() {
		return b.keys[i]
	}
	if b.isLeaf {
		return nil
	}
	return b.children[i].Search(k)
}
func (b *BTreeNode) Traverse() []datatype.DataType {
	dataList := make([]datatype.DataType, 0)
	var i = 0
	for i = 0; i < b.n; i++ {
		if b.isLeaf == false {
			temp := b.children[i].Traverse()
			for j := 0; j < len(temp); j++ {
				dataList = append(dataList, temp[j])
			}

		}
		dataList = append(dataList, *b.keys[i])

	}
	if b.isLeaf == false {
		temp := b.children[i].Traverse()
		for j := 0; j < len(temp); j++ {
			dataList = append(dataList, temp[j])
		}
	}
	return dataList
}

func (b *BTreeNode) GetByPrefix(n *int, prefix string) []*datatype.DataType {
	var dataList []*datatype.DataType
	var i = 0
	for i = 0; i < b.n; i++ {
		if b.isLeaf == false {
			temp := b.children[i].GetByPrefix(n, prefix)
			for j := 0; j < len(temp); j++ {
				if strings.HasPrefix(temp[i].GetKey(), prefix) && !temp[i].IsDeleted() {
					if *n == 0 {
						return dataList
					}
					dataList = append(dataList, temp[j])
					*n--
				}

			}

		}
		dataList = append(dataList, b.keys[i])

	}
	if b.isLeaf == false {
		temp := b.children[i].GetByPrefix(n, prefix)
		for j := 0; j < len(temp); j++ {
			dataList = append(dataList, temp[j])
		}
	}
	return dataList
}
func (b *BTreeNode) GetByRange(n *int, valRange []string) []*datatype.DataType {
	var dataList []*datatype.DataType
	var i = 0
	for i = 0; i < b.n; i++ {
		if b.isLeaf == false {
			temp := b.children[i].GetByRange(n, valRange)
			for j := 0; j < len(temp); j++ {
				if isInRange(temp[i].GetKey(), valRange) && !temp[i].IsDeleted() {
					if *n == 0 {
						return dataList
					}
					dataList = append(dataList, temp[j])
					*n--

				}

			}

		}
		dataList = append(dataList, b.keys[i])

	}
	if b.isLeaf == false {
		temp := b.children[i].GetByRange(n, valRange)
		for j := 0; j < len(temp); j++ {
			dataList = append(dataList, temp[j])
		}
	}
	return dataList
}
func (b *BTreeNode) T() int {
	return b.t
}

func (b *BTreeNode) SetT(t int) {
	b.t = t
}

func (b *BTreeNode) IsLeaf() bool {
	return b.isLeaf
}

func (b *BTreeNode) SetIsLeaf(isLeaf bool) {
	b.isLeaf = isLeaf
}

func (b *BTreeNode) Keys() []*datatype.DataType {
	return b.keys
}

func (b *BTreeNode) SetKeys(keys []*datatype.DataType) {
	b.keys = keys
}

func (b *BTreeNode) Children() []*BTreeNode {
	return b.children
}

func (b *BTreeNode) SetChildren(children []*BTreeNode) {
	b.children = children
}

func (b *BTreeNode) N() int {
	return b.n
}

func (b *BTreeNode) SetN(n int) {
	b.n = n
}
