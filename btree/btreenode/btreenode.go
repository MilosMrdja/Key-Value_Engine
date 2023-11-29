package btreenode

import (
	"awesomeProject/btreenode/datatype"
)

type BTreeNode struct {
	IsLeaf   bool
	Keys     []*datatype.DataType
	Children []*BTreeNode
}

func NewBTreeNode(leaf bool) *BTreeNode {
	return &BTreeNode{
		IsLeaf: leaf,
	}
}
