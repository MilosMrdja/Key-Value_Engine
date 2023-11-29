package MerkleTree

import (
	"encoding/binary"
	"fmt"
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
	tree       []*Node // niz pokazivaca na Node-ove
	merkleRoot *Node   // pokazivac na glavu
	numOfData  int     // broj podataka
	height     int     // visina stabla
}

// konstruktor - [root, ...., skroz levo dete,..., skroz desno dete]
func CreateMerkleTree(data [][]byte) (*MerkleTree, error) {
	var numLeafs, numNodes, hTree int
	numLeafs = 0
	numNodes = 0
	hTree = 0
	if len(data) == 0 {
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
	MTree.numOfData = len(data)

	return MTree, err
}

// f-ja koja popunjava merkle tree odredjenim hash vrednostima
func fillMerkleTree(numN int, data [][]byte, numL int) (*MerkleTree, error) {
	merkleTree := &MerkleTree{
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
			_, err := hash.Write(data[brData])
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

// f-ja koja vraca head element
func GetMerkleRoot(mt *MerkleTree) *Node {
	return mt.merkleRoot
}

// f-ja vraca broj elemenata u merkle stablu
func GetNumNodes(mt *MerkleTree) int {
	return len(mt.tree)
}

// f-ja koja vraca broj podatak - broj listova cija vrednost razlicita od nule
func GetNumData(mt *MerkleTree) int {
	return mt.numOfData
}

// f-ja koja vraca visinu stabla
func getHeightOfMerkleTree(mt *MerkleTree) int {
	return mt.height
}

// serijalizacija merkle stabla - serijalizujemo duzinu niza i N hesirane vrednosti
// {1B, 8B, 8B,...,8B}
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
// deserijalizujemo duzinu niza kao prvi bajt i svaki 8B kao hesiranu vrednost cvora
// posle namestamo pokazivace
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
		merkleRoot: nil,
		numOfData:  0,
		height:     0,
	}

	numOfData := 0
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
		if tempNode.hashValue != 0 && i > int(duzina[0])/2-1 {
			numOfData += 1
		}
		Mtree.tree[i] = &tempNode
	}

	for i := 0; 2*i+1 < int(duzina[0]); i++ {
		Mtree.tree[i].left = Mtree.tree[2*i+1]
		Mtree.tree[i].right = Mtree.tree[2*i+2]
	}

	Mtree.merkleRoot = Mtree.tree[0]
	Mtree.numOfData = numOfData
	Mtree.height = int(math.Log2(float64(duzina[0])))
	return &Mtree, true, nil
}

// f-ja ispisuje samo hesiranu vrednost node-a, bez ispisa roditelja
func PrintNode(node *Node) string {
	return fmt.Sprint(node.hashValue)
}

// f-ja koja vraca string kao podatke
func PrintMerkleTree(mt *MerkleTree) string {
	res := "\nVisina merkle stabla: " + fmt.Sprint(mt.height)
	res += "\nBroj cvorova u merkle stablu: " + fmt.Sprint(GetNumNodes(mt))
	res += "\nBroj podataka u merkle stablu: " + fmt.Sprint(mt.numOfData)
	res += "\nIspisane hash vrednosti cvorova(head, levo dete, desno dete): \n"
	for i := 0; i < len(mt.tree); i++ {
		res += PrintNode(mt.tree[i])
		if i != len(mt.tree)-1 {
			res += ","
		}
	}
	return res
}

// f-ja koja uporedjuje dva stabla i vraca niz indeksa od elemenata cije su vrednosti promenjene
// prvo stablo je originalno, a drugo da li je doslo do promene neke vrednosti
// return nil -> stabla su ista
// return len(array) > 0    ->   podaci su se negde promenili
// return boo: true -> uporedjivanje je izvrseno, false -> uporedjivanje nije izvrsenog zbog necega
func CheckChanges(mt1 *MerkleTree, mt2 *MerkleTree) ([]uint64, bool) {

	// ako nisu iste visine nema smisla da proveravamo
	if mt1.height != mt2.height {
		return nil, false
	}

	var res []uint64                                           // rezultat koji vracam
	var valuesByGivenLevel func(node *Node, level int) []*Node // rekurzija kroz stablo
	var hashArr []*Node                                        // pomocna za pisanje hash vrednosti na odredjenom level-u

	valuesByGivenLevel = func(node *Node, level int) []*Node {

		if node == nil {
			return hashArr
		}
		if level == 1 {
			hashArr = append(hashArr, node)
		} else if level > 1 {
			valuesByGivenLevel(node.left, level-1)
			valuesByGivenLevel(node.right, level-1)
		}
		return hashArr
	}

	tempRes := make([]*Node, 2)
	tempRes[0] = mt1.merkleRoot
	tempRes[1] = mt2.merkleRoot
	var temp1 []*Node
	var temp2 []*Node
	for len(tempRes) != 0 {
		if tempRes[0].hashValue == tempRes[1].hashValue {
			return nil, true
		}
		if tempRes[0].left == nil {
			for k := 0; k < len(tempRes); k++ {
				res = append(res, tempRes[k].hashValue)
			}
			break
		}
		temp1 = valuesByGivenLevel(tempRes[0], 2)
		hashArr = nil
		temp2 = valuesByGivenLevel(tempRes[1], 2)
		hashArr = nil

		tempRes = tempRes[2:] // uklanjamo prva dva elementa

		for i := 0; i < len(temp1); i++ {
			if temp1[i].hashValue != temp2[i].hashValue {
				tempRes = append(tempRes, temp1[i])
				tempRes = append(tempRes, temp2[i])
			}
		}
	}

	return res, true
}
