package btreenode

import "awesomeProject/btreenode/nodeelement"

type BTreeNode struct {
	IsLeaf   bool
	Keys     []*nodeelement.NodeElement
	Children []*BTreeNode
}

func NewBTreeNode(list bool) *BTreeNode {
	return &BTreeNode{
		IsLeaf: list,
	}
}
