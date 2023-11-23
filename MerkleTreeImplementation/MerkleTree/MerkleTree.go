package MerkleTree

import (
	"fmt"
	"hash/fnv"
	"math"
)

type Node struct {
	left      *Node
	right     *Node
	hashValue uint64
}

type MerkleTree struct {
	tree       []*Node
	merkleRoot *Node
	numOfData  int
}

func CreateMerkleTree(data []string) (*MerkleTree, error) {
	var numLeafs, numNodes int
	numLeafs = 0
	numNodes = 0
	if len(data) == 0 {
		numLeafs = 0
		numNodes = 0
		return nil, nil
	} else if len(data) == 1 {
		numLeafs = 0
		numNodes = 1
	} else {
		var i int

		for i = 1; numLeafs < len(data); i++ {
			numLeafs = 1 << i
			numNodes += int(math.Pow(2, float64(i-1)))
		}
		numNodes += numLeafs
		fmt.Println(numLeafs, numNodes)
	}
	MTree, err := fillMerkleTree(numNodes, data, numLeafs)
	for i := 0; i < len(MTree.tree); i++ {
		fmt.Println(MTree.tree[i].hashValue)
	}

	return MTree, err
}

func fillMerkleTree(numN int, data []string, numL int) (*MerkleTree, error) {
	merkleTree := &MerkleTree{
		numOfData:  numN,
		merkleRoot: nil,
		tree:       make([]*Node, numN),
	}
	hash := fnv.New32()
	brData := 0
	for i := numN - numL; i < len(merkleTree.tree); i++ {

		tempNode := Node{
			left:  nil,
			right: nil,
		}
		if brData < len(data) {
			_, err := hash.Write([]byte(data[brData]))
			if err != nil {
				return nil, err
			}
			tempNode.hashValue = uint64(hash.Sum32())
		} else {
			tempNode.hashValue = 0
		}

		merkleTree.tree[i] = &tempNode
		brData += 1
	}

	brData = len(merkleTree.tree) - 1
	for merkleTree.tree[0] == nil {

		tempNode := Node{
			left:      merkleTree.tree[brData-1],
			right:     merkleTree.tree[brData],
			hashValue: merkleTree.tree[brData].hashValue + merkleTree.tree[brData-1].hashValue,
		}
		merkleTree.tree[brData/2-1] = &tempNode
		brData -= 2
	}
	merkleTree.merkleRoot = merkleTree.tree[0]

	return merkleTree, nil
}
