package main

import (
	"awesomeProject/btree"
	"awesomeProject/btreenode"
	"awesomeProject/btreenode/datatype"
	"fmt"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
func main() {
	tree := btree.NewBTree(btreenode.NewBTreeNode(true), 2)
	testString := RandStringRunes(10)
	testObj := datatype.CreateDataType(testString, make([]byte, 2))
	tree.InsertTreeNode(testObj)
	for i := 1; i < 10; i++ {
		pom := RandStringRunes(10)
		testString = pom
		tree.InsertTreeNode(datatype.CreateDataType(testString, make([]byte, 2)))
	}

	output, outputNode, _ := tree.SearchKeyFromRoot(testString)
	fmt.Println(output.IsLeaf)
	fmt.Println(outputNode.GetKey())
}
