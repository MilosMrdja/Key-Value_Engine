package MerkleTree

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"os"
)

type Node struct {
	left      *Node
	right     *Node
	hashValue uint64
}

type MerkleTree struct {
	tree       []*Node
	leafs      []*Node
	merkleRoot *Node
	numOfData  int
	height     int
}

// konstruktor - [root, ...., skroz levo dete,..., skroz desno dete]
func CreateMerkleTree(data []string) (*MerkleTree, error) {
	var numLeafs, numNodes, hTree int
	numLeafs = 0
	numNodes = 0
	hTree = 0
	if len(data) == 0 {
		numLeafs = 0
		numNodes = 0
		hTree = 0
		return nil, nil
	} else if len(data) == 1 {
		numLeafs = 1
		numNodes = 1
		hTree = 0
	} else {
		var i int

		//dovrsavamo broj listova do najveceg najblizeg 2^n broja
		for i = 1; numLeafs < len(data); i++ {
			numLeafs = 1 << i
			numNodes += int(math.Pow(2, float64(i-1)))
			hTree += 1
		}
		numNodes += numLeafs

	}
	MTree, err := fillMerkleTree(numNodes, data, numLeafs)
	MTree.height = hTree
	MTree.leafs = make([]*Node, numLeafs)

	for i := 0; i < numLeafs; i++ {
		MTree.leafs[i] = MTree.tree[numNodes-numLeafs+i]
	}

	return MTree, err
}

// f-ja koja popunjava merkle tree odredjenim hash vrednostima
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
		//za ulazni niz podataka dodeljuje hes vrednost, za ostale postavlja na 0
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

	// kada smo postavili lsitove, na osnovu njih inicijalizujemo njihove roditelje
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

// f-ja koja vraca niz listova
func GetLeafs(mt *MerkleTree) []*Node {
	return mt.leafs
}

// f-ja koja vraca head element
func GetMerkleRoot(mt *MerkleTree) *Node {
	return mt.merkleRoot
}

// f-ja vraca broj elemenata u merkle stablu
func GetNumNodes(mt *MerkleTree) int {
	return mt.numOfData
}

// f-ja vraca broj listova
func GetNumLeafs(mt *MerkleTree) int {
	return len(mt.leafs)
}

// serijalizacija merkle stabla
func SerializeMerkleTree(mt *MerkleTree) (bool, error) {
	fileName := "MerkleTree.bin"
	_, err := os.Stat(fileName)
	if err == nil {
		err1 := os.Remove(fileName)
		if err1 != nil {
			return false, err1
		}
	}

	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0777)

	defer file.Close()

	duzinaNiza := make([]byte, 1)
	duzinaNiza[0] = byte(len(mt.tree))
	_, errF := file.Write(duzinaNiza)
	if errF != nil {
		return false, errF
	}

	for i := 0; i < len(mt.tree); i++ {
		bytes := make([]byte, 8)
		binary.BigEndian.PutUint64(bytes, mt.tree[i].hashValue)
		_, err := file.Write(bytes)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// deserijalizacija merkle stabla
func DeserializeMerkleTree(fileName string) (*MerkleTree, bool, error) {
	_, err := os.Stat(fileName)
	if err != nil {
		return nil, false, err
	}
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0777)
	if err != nil {
		return nil, false, err
	}

	_, err = file.Seek(0, 0) //da dodjemo na pocetak

	if err != nil {
		return nil, false, err
	}
	duzina := make([]byte, 1)
	_, err = file.Read(duzina)
	if err != nil {
		return nil, false, err
	}

	Mtree := MerkleTree{
		tree:       make([]*Node, duzina[0]),
		leafs:      nil,
		merkleRoot: nil,
		numOfData:  0,
		height:     0,
	}

	for i := 0; i < int(duzina[0]); i++ {
		tempNode := Node{
			left:      nil,
			right:     nil,
			hashValue: 0,
		}
		tempHash := make([]byte, 8)
		err := binary.Read(file, binary.BigEndian, tempHash)
		if err != nil {
			return nil, false, err
		}
		tempNode.hashValue = binary.BigEndian.Uint64(tempHash)
		Mtree.tree[i] = &tempNode
	}

	for i := 0; 2*i+1 < int(duzina[0]); i++ {
		Mtree.tree[i].left = Mtree.tree[2*i+1]
		Mtree.tree[i].right = Mtree.tree[2*i+2]
	}

	Mtree.merkleRoot = Mtree.tree[0]
	Mtree.numOfData = len(Mtree.tree)
	return &Mtree, true, nil
}

// vraca da li je doslo do izmena nad podacima
// TRUE -> nije doslo do izmene podatka, i dalje mu je ista hash vrednost
// FALSE -> doslo je do izmene nad prosledjenim podatkom
func checkChanges(mt *MerkleTree, data string) (bool, error) {
	hash := fnv.New32()
	_, err := hash.Write([]byte(data))

	if err != nil {
		return false, err
	}
	for i := 0; i < GetNumLeafs(mt); i++ {
		if mt.tree[i].hashValue == uint64(hash.Sum32()) {
			return true, nil
		}

	}
	return false, nil
}
