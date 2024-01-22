package main

import (
	"fmt"
	"sstable/MerkleTreeImplementation/MerkleTree"
)

func main() {
	fmt.Println("-----------------------------------------------------------------------------------------------------------------------")
	fmt.Println("Merkle tree")
	fmt.Println("-----------------------------------------------------------------------------------------------------------------------")
	testArray := [][]byte{
		{1, 2, 3, 4, 5, 6},
		{1, 1, 1, 1},
		{23, 23, 33, 32},
		{0, 0, 0, 0},
		{0, 1, 1, 1, 1, 0},
		{2, 3, 34, 1, 2},
		{3, 8, 6, 5, 5},
		{3, 8, 6, 5, 5},
		{0, 1, 1, 1, 1, 0},
		{2, 3, 34, 1, 2},
		{3, 8, 6, 5, 5},
		{3, 8, 6, 5, 5},
	}
	testArray2 := [][]byte{
		{1, 2, 3, 4, 5, 6},
		{1, 1, 1, 1},
		{23, 23, 33, 32},
		{0, 0, 0, 0},
		{0, 1, 1, 1, 1, 0},
		{2, 3, 34, 1, 2},
		{3, 8, 6, 5, 5},
		{3, 8, 6, 5, 5},
		{0, 1, 1, 1, 1, 0},
		{2, 3, 34, 1, 2},
		{3, 8, 6, 5, 5},
		{0, 0, 0, 0, 0},
	}

	mTest, err := MerkleTree.CreateMerkleTree(testArray2)
	if err != nil {
		fmt.Println(err)
	}

	m, err := MerkleTree.CreateMerkleTree(testArray)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Kreirano merkle stablo: ")
	fmt.Println(MerkleTree.PrintMerkleTree(m))
	_, err1 := MerkleTree.SerializeMerkleTree(m, "MerkleTree.bin")
	if err1 != nil {
		fmt.Println("Nije doslo do serijalizacije merkle stabla!")
		fmt.Println(err1)
	}

	fmt.Println("\n-----------------------------------------------------------------------------------------------------------------------")
	fmt.Println("Serijalizovano i deserijalizovano to isto merkle stablo , uporedjivanje vrednosti: ")
	m1, _, _ := MerkleTree.DeserializeMerkleTree("MerkleTree.bin")
	fmt.Println(MerkleTree.PrintMerkleTree(m1))
	fmt.Println("\n-----------------------------------------------------------------------------------------------------------------------")
	fmt.Println("MerkleTree m - Test:")
	fmt.Println(MerkleTree.PrintMerkleTree(mTest))
	fmt.Println("\n-----------------------------------------------------------------------------------------------------------------------")
	fmt.Println("MerkleTree m - Real:")
	fmt.Println(MerkleTree.PrintMerkleTree(m))
	fmt.Println("\n-----------------------------------------------------------------------------------------------------------------------")
	testArr, check := MerkleTree.CheckChanges(m, mTest)
	if !check {
		fmt.Println("Doslo je do nepodudaranja izmedju dva merkle stabla")
	} else {
		fmt.Println(testArr)
	}
	realArr, check := MerkleTree.CheckChanges(m, m1)
	if !check {
		fmt.Println("Doslo je do nepodudaranja izmedju dva merkle stabla")
	} else {
		fmt.Println(realArr)
	}
}
