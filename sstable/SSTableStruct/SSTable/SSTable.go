package SSTable

import (
	"os"
	"sstable/MerkleTreeImplementation/MerkleTree"
	"sstable/bloomfilter/bloomfilter"
	"sstable/mem/memtable/datatype"
)

type SSTable struct {
	bloomFilter *bloomfilter.BloomFilter //referenca?
	merkleTree  *MerkleTree.MerkleTree
	summary     map[string]int
	index       map[string]int
	data        []byte
}

// N i M su nam redom razudjenost u index-u, i u summary-ju
func NewSSTable(dataList []datatype.DataType, N, M int) bool {

	// pomocne promenljive
	arrToMerkle := make([][]byte, 0)
	var serializedData []byte
	var duzinaPodatka, acc, duzinaDataList int
	acc = 0
	duzinaDataList = len(dataList)

	sstable := &SSTable{
		bloomFilter: bloomfilter.CreateBloomFilter(duzinaDataList),
		merkleTree:  nil,
		index:       make(map[string]int),
		summary:     make(map[string]int),
		data:        make([]byte, 0),
	}

	fileName := "sstable/data/SSTable1.bin"
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return false
	}
	defer file.Close()

	// glavna petlja
	for i := 0; i < duzinaDataList; i++ {
		// dodali smo kljuc u bloomf
		AddKeyToBloomFilter(dataList[i].GetKey())

		// serijaliacija podatka
		serializedData = SerializeDataType(dataList[i])
		duzinaPodatka, err = file.Write(serializedData)
		//sstable.data = append(sstable.data,serializedData...)
		if err != nil {
			return false
		}
		if i%N == 0 || i+1 == duzinaDataList {
			sstable.index[dataList[i].GetKey()] = acc
		}
		acc += duzinaPodatka

		// pomocni niz koji presludjemo za MerkleTree
		arrToMerkle = append(arrToMerkle, serializedData)

	}
	CreateMerkleTree(arrToMerkle)

	return true
}

// f-ja koja kreira merkle stablo, vraca True ako je uspesno kreirano, u suprotnom False
func CreateMerkleTree(data [][]byte) bool {
	return true
}

// f-ja koja serijalizuje jedan podatak iz memtabele
func SerializeDataType(data datatype.DataType) []byte {
	res := make([]byte, 0)
	return res
}

// f-ja koja dodaje kljuc u bloomfilter i vraca True ako je uspesno dodao
func AddKeyToBloomFilter(key string) bool {
	return true
}

// f-ja koja proverava da li kljuc postoji
func IfKeyExist(key string) bool {
	return true
}
