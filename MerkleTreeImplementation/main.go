package main

import (
	"MerkleTreeImplementation/MerkleTree"
	"fmt"
)

func main() {
	fmt.Println("---------------------------------------------------------------------------")
	fmt.Println("Merkle tree")
	fmt.Println("---------------------------------------------------------------------------")
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
	m, err := MerkleTree.CreateMerkleTree(testArray)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Kreirano merkle stablo: ")
	MerkleTree.PrintMerkleTree(m)
	_, err1 := MerkleTree.SerializeMerkleTree(m)
	if err1 != nil {
		fmt.Println("Nije doslo do serijalizacije merkle stabla!")
		fmt.Println(err1)
	}

	fmt.Println("\n---------------------------------------------------------------------------")
	fmt.Println("Serijalizovano i deserijalizovano to isto merkle stablo , uporedjivanje vrednosti: ")
	m1, _, _ := MerkleTree.DeserializeMerkleTree("MerkleTree.bin")
	MerkleTree.PrintMerkleTree(m1)
	fmt.Println("\n---------------------------------------------------------------------------")

}
