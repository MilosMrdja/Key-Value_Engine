package main

import (
	"awesomeProject/btree"
	"awesomeProject/btreenode"
	"awesomeProject/btreenode/datatype"
	"awesomeProject/btreenode/nodeelement"
	"fmt"
	"math/rand"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}
func main() {
	tree := btree.NewBTree(btreenode.NewBTreeNode(true), 3)
	testString := string(rune(-1 + 48))
	testObj := nodeelement.NewNodeElement(testString, datatype.CreateDataType(testString, make([]byte, 2)))
	tree.InsertTreeNode(testObj)
	for i := 0; i < 10; i++ {
		pom := string(rune(i + 48))
		testString = pom
		tree.InsertTreeNode(nodeelement.NewNodeElement(testString, datatype.CreateDataType(testString, make([]byte, 2))))
	}

	tree.PrintTree(tree.Root, 0)
	output, _ := tree.SearchKeyFromRoot(testString)
	fmt.Println(output.IsLeaf)
}
