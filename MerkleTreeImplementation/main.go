package main

import (
	"MerkleTreeImplementation/MerkleTree"
	"fmt"
)

func main() {
	fmt.Println("Merkle tree")
	testArray := make([]string, 7)
	testArray[0] = "milos"
	testArray[1] = "milos1"
	testArray[2] = "milos2"
	testArray[3] = "milos3"
	testArray[4] = "milos4"
	testArray[5] = "milos5"
	testArray[6] = "milos6"
	_, err := MerkleTree.CreateMerkleTree(testArray)
	if err != nil {
		fmt.Println(err)
	}
	//check, err1 := MerkleTree.SerializeMerkleTree(m)
	//fmt.Println(check, err1)
	//m1, check, _ := MerkleTree.DeserializeMerkleTree("MerkleTree.bin")
	//fmt.Println(check)
	//MerkleTree.PrintHashValues(m1)
}
