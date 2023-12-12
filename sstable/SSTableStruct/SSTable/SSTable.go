package SSTable

import (
	"sstable/MerkleTreeImplementation/MerkleTree"
	"sstable/bloomfilter/bloomfilter"
)

type SSTable struct {
	bloomFilter *bloomfilter.BloomFilter //referenca?
	merkleTree  *MerkleTree.MerkleTree
	summary     map[string]int
	index       map[string]int
	data        []byte
}

func NewSSTable(dataList []byte) bool {
	// poz = 0 // raditi sa data[]byte
	// brojac za indeks i za summar == brSum, brIndex
	// for za svaki element u dataList

	// kljuc kroz bloom, on se sada nalazi u bloom-u
	// serijalizacija za taj el, -> data
	// if brIndex % N1 == 0
	// pravimo index(imamo kljuc iz DATA, offset(poz)
	// poz += len(serijalizovanog el)
	// id brSum % N2 == 0
	// pravimo summary(index od indexa)

	// [][]byte pravimo za merkle
	// pravimo merkle
	return true
}

// f-ja koja prima kljuc i propusta key kroz bloom
func KeyExist(key string)bool{
	return true
}
