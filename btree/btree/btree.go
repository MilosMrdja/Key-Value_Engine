package btree

import (
	"awesomeProject/btreenode"
	"awesomeProject/btreenode/datatype"
	"fmt"
)
import "awesomeProject/myutils"

type BTree struct {
	Root *btreenode.BTreeNode
	Rang uint64
}

func NewBTree(root *btreenode.BTreeNode, t uint64) *BTree {
	return &BTree{Root: root, Rang: t}
}

func (tree *BTree) InsertTreeNode(key *datatype.DataType) {
	root := tree.Root
	if uint64(len(root.Keys)) == tree.Rang*2-1 {
		temp := btreenode.NewBTreeNode(false)
		tree.Root = temp
		temp.Children = myutils.Insert(temp.Children, 0, root)
		tree.SplitChild(temp, 0)
		tree.InsertNonFull(temp, key)

	} else {
		tree.InsertNonFull(root, key)
	}
}

func (tree *BTree) InsertNonFull(node *btreenode.BTreeNode, key *datatype.DataType) {
	i := len(node.Keys) - 1
	if node.IsLeaf {
		pom := datatype.CreateDataType("null", make([]byte, 0))
		node.Keys = append(node.Keys, pom)
		for i >= 0 && key.GetKey() < node.Keys[i].GetKey() {
			node.Keys[i+1] = node.Keys[i]
			i--
		}
		node.Keys[i+1] = key

	} else {
		for i >= 0 && key.GetKey()[i] < node.Keys[i].GetKey()[i] {
			i--

		}
		i++
		if uint64(len(node.Children[i].Keys)) == 2*tree.Rang-1 {
			tree.SplitChild(node, i)
			if key.GetKey()[i] > node.Keys[i].GetKey()[i] {
				i++

			}
		}
		tree.InsertNonFull(node.Children[i], key)
	}
}

func (tree *BTree) SplitChild(node *btreenode.BTreeNode, index int) {
	t := tree.Rang
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

func (tree *BTree) SearchKeyFromRoot(key string) (*btreenode.BTreeNode, *datatype.DataType, bool) {
	return tree.SearchKeyFromNode(key, tree.Root)
}

func (tree *BTree) SearchKeyFromNode(key string, node *btreenode.BTreeNode) (*btreenode.BTreeNode, *datatype.DataType, bool) {
	if node != nil {
		i := 0
		for i < len(node.Keys) && key[i] > node.Keys[i].GetKey()[i] {
			i++
		}
		if i < len(node.Keys) && key[i] == node.Keys[i].GetKey()[i] {
			return node, node.Keys[i], true
		} else if node.IsLeaf {
			return nil, nil, false
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
