package btree

import (
	"awesomeProject/btreenode"
	"awesomeProject/btreenode/datatype"
	"awesomeProject/btreenode/nodeelement"
	"fmt"
)
import "awesomeProject/myutils"

type BTree struct {
	Root  *btreenode.BTreeNode
	Depth uint64
}

func NewBTree(root *btreenode.BTreeNode, t uint64) *BTree {
	return &BTree{Root: root, Depth: t}
}

func (tree *BTree) InsertTreeNode(key *nodeelement.NodeElement) {
	root := tree.Root
	if uint64(len(root.Keys)) == tree.Depth*2+1 {
		temp := btreenode.NewBTreeNode(false)
		tree.Root = temp
		temp.Children = myutils.Insert(temp.Children, 0, root)
		tree.SplitChild(temp, 0)
		tree.InsertNonFull(temp, key)

	} else {
		tree.InsertNonFull(root, key)
	}
}

func (tree *BTree) InsertNonFull(node *btreenode.BTreeNode, key *nodeelement.NodeElement) {
	i := len(node.Keys) - 1
	if node.IsLeaf {
		pom := nodeelement.NewNodeElement("null", datatype.CreateDataType("null", make([]byte, 0)))
		node.Keys = append(node.Keys, pom)
		for ; i >= 0 && key.GetKey() < node.Keys[i].GetKey(); i-- {
			node.Keys[i+1] = node.Keys[i]

		}
		node.Keys[i+1].SetObj(key.GetObj())
	} else {
		for i >= 0 && key.GetKey() < node.Keys[i].GetKey() {
			i--

		}
		i++
		if uint64(len(node.Children[i].Keys)) == 2*tree.Depth-1 {
			tree.SplitChild(node, i)
			if key.GetKey() > node.Keys[i].GetKey() {
				i++

			}
		}
		tree.InsertNonFull(node.Children[i], key)
	}
}

func (tree *BTree) SplitChild(node *btreenode.BTreeNode, index int) {
	t := tree.Depth
	y := node.Children[index]
	z := btreenode.NewBTreeNode(y.IsLeaf)
	//node.child.insert(i + 1, z)
	node.Children = myutils.Insert(node.Children, index+1, z)
	//x.keys.insert(i, y.keys[t - 1])
	node.Keys = myutils.Insert(node.Keys, index, y.Keys[t-1])
	z.Keys = y.Keys[t : (2*t)-1]
	y.Keys = y.Keys[0 : t-1]
	if !y.IsLeaf {
		z.Children = y.Children[t : 2*t]
		y.Children = y.Children[0 : t-1]

	}
}

func (tree *BTree) SearchKeyFromRoot(key string) (*btreenode.BTreeNode, int) {
	return tree.SearchKeyFromNode(key, tree.Root)
}

func (tree *BTree) SearchKeyFromNode(key string, node *btreenode.BTreeNode) (*btreenode.BTreeNode, int) {
	if node != nil {
		i := 0
		for i < len(node.Keys) && key > node.Keys[i].GetKey() {
			i++
		}
		if i < len(node.Keys) && key == node.Keys[i].GetKey() {
			return node, i
		} else if node.IsLeaf {
			return nil, -100
		} else {
			return tree.SearchKeyFromNode(key, node.Children[i])
		}

	} else {
		return tree.SearchKeyFromNode(key, tree.Root)
	}
}

func (tree *BTree) PrintTree(node *btreenode.BTreeNode, index int) {
	fmt.Print("Level ")
	fmt.Print(index)
	fmt.Print(" N")
	fmt.Print(len(node.Keys))
	fmt.Print(": ")
	for i := 0; i < len(node.Keys); i++ {
		fmt.Print(i)
		fmt.Print(" ")
	}
	fmt.Println(" ")
	index++
	if len(node.Children) > 0 {
		for i := 0; i < len(node.Children); i++ {
			tree.PrintTree(node.Children[i], index)
		}
	}
}
