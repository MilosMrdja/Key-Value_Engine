package SSTable

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
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
	dictionary  map[string]int32 // jel ok 32 btina vrednost
}

// N i M su nam redom razudjenost u index-u, i u summary-ju
func NewSSTable(dataList []datatype.DataType, N, M int, fileName string, compress1, compress2, oneFile bool) bool {

	// pomocne promenljive
	arrToMerkle := make([][]byte, 0)
	var serializedData, indexData []byte
	var duzinaPodatka, acc, accIndex, duzinaDataList int
	acc = 0
	accIndex = 0
	duzinaDataList = len(dataList)
	var err error
	bloomFilter := bloomfilter.CreateBloomFilter(duzinaDataList)
	// mapa za enkodirane vrednosti

	dictionary := make(map[string]int32)

	//Data fajl
	file, err := os.OpenFile(fileName+"/Data.bin", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return false
	}
	defer file.Close()

	//Index fajl
	fileIndex, err2 := os.OpenFile(fileName+"/Index.bin", os.O_WRONLY|os.O_CREATE, 0666)
	if err2 != nil {
		return false
	}
	defer fileIndex.Close()

	//Symmary fajl
	fileSummary, err3 := os.OpenFile(fileName+"/Summary.bin", os.O_WRONLY|os.O_CREATE, 0666)
	if err3 != nil {
		return false
	}
	defer fileSummary.Close()

	//Bloom Filter fajl
	// glavna petlja

	for i := 0; i < duzinaDataList; i++ {
		// u mapu dodamo encodiranu vrednost kljuca
		dictionary[dataList[i].GetKey()] = int32(i)
		// dodali smo kljuc u bloomf
		AddKeyToBloomFilter(bloomFilter, dataList[i].GetKey())

		// serijaliacija podatka
		serializedData, err = SerializeDataType(dataList[i], compress1, compress2, int32(i))
		if err != nil {
			return false
		}

		// upisujemo podatak u Data.bin fajl
		duzinaPodatka, err = file.Write(serializedData)
		if err != nil {
			return false
		}

		//Upis odgovarajucih vrednosti u Summary
		if (i+1)%M == 0 {
			indexData, err = SerializeIndexData(dataList[i].GetKey(), accIndex, compress1, compress2, int32(i))
			if err != nil {
				return false
			}
			fileSummary.Write(indexData)
		}
		//Upis odgovarajucih vrednosti u Index
		if (i+1)%N == 0 {
			indexData, err = SerializeIndexData(dataList[i].GetKey(), acc, compress1, compress2, int32(i))
			if err != nil {
				return false
			}
			fileIndex.Write(indexData)
			accIndex += len(indexData)

		}

		acc += duzinaPodatka

		// pomocni niz koji presludjemo za MerkleTree
		arrToMerkle = append(arrToMerkle, serializedData)

	}
	//Kreiranje i upis Merkle Stabla
	CreateMerkleTree(arrToMerkle, fileName+"/Merkle.bin")

	//Serijalizacija i upis bloom filtera
	err = bloomfilter.SerializeBloomFilter(bloomFilter, fileName+"/BloomFilter.bin")
	if err != nil {
		return false
	}
	//fmt.Printf("%d\n", acc)

	//serijalizacija hash mape
	hashFileName := fileName + "/HashMap.bin"
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err = e.Encode(dictionary)
	if err != nil {
		panic(err)
	}
	err = SerializeHashmap(hashFileName, b.Bytes())
	if err != nil {
		panic(err)
	}

	// u slucaju da korisnik odabere sve u jedan fajl
	if oneFile {
		var serializedInOneFile []byte
		if compress2 {
			serializedInOneFile, err = WriteToOneFile(fileName+"/BloomFilter.bin", fileName+"/Hash.bin", fileName+"/Summary.bin", fileName+"/Index.bin", fileName+"/Data.bin", fileName+"/Merkle.bin")
			if err != nil {
				panic(err)
			}
		} else {
			serializedInOneFile, err = WriteToOneFile(fileName+"/BloomFilter.bin", "", fileName+"/Summary.bin", fileName+"/Index.bin", fileName+"/Data.bin", fileName+"/Merkle.bin")
			if err != nil {
				panic(err)
			}
		}

		// One file
		fileNameOneFile := fileName + "/SSTable.bin"
		fileOne, err2 := os.OpenFile(fileNameOneFile, os.O_WRONLY|os.O_CREATE, 0666)
		if err2 != nil {
			fmt.Println("Adsas")
		}
		defer fileOne.Close()
		fileOne.Write(serializedInOneFile)
	}

	// printanje recnika
	bs, _ := json.Marshal(dictionary)
	fmt.Println(string(bs))

	return true
}

// f-ja koja kreira merkle stablo, vraca True ako je uspesno kreirano, u suprotnom False
func CreateMerkleTree(data [][]byte, fileName string) bool {
	merkleTree, err := MerkleTree.CreateMerkleTree(data)

	if err != nil {
		return false
	}
	_, err3 := MerkleTree.SerializeMerkleTree(merkleTree, fileName)
	if err3 != nil {
		return false
	}
	return true
}

// f-ja koja dodaje kljuc u bloomfilter i vraca True ako je uspesno dodao
func AddKeyToBloomFilter(bloomFilter *bloomfilter.BloomFilter, key string) bool {
	bloomFilter.Set([]byte(key))
	return true
}

func positionInSSTable(file os.File, position int) (int64, int64) {
	var bytes []byte
	var size, sizeEnd int64
	file.Seek(0, 0)
	size = 0
	for i := 0; i < position; i++ {
		bytes = make([]byte, 8)
		_, err := file.Read(bytes)
		if err != nil {
			panic(err)
		}
		size += 8
		size += int64(binary.BigEndian.Uint64(bytes))

		file.Seek(size, 0)
	}
	bytes = make([]byte, 8)
	_, err := file.Read(bytes)
	if err != nil {
		panic(err)
	}
	size += 8
	sizeEnd = size + int64(binary.BigEndian.Uint64(bytes))

	return size, sizeEnd
}
