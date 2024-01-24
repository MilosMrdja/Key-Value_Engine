package SSTable

import (
	"encoding/binary"
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
}

// N i M su nam redom razudjenost u index-u, i u summary-ju
func NewSSTable(dataList []datatype.DataType, N, M int, compres bool) bool {

	// pomocne promenljive
	arrToMerkle := make([][]byte, 0)
	var serializedData, indexData []byte
	var duzinaPodatka, acc, accIndex, duzinaDataList int
	acc = 0
	accIndex = 0
	duzinaDataList = len(dataList)
	var err error
	bloomFilter := bloomfilter.CreateBloomFilter(duzinaDataList)

	//Data fajl
	fileName := "DataSSTable/Data.bin"
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return false
	}
	defer file.Close()

	//Index fajl
	fileName = "DataSSTable/Index.bin"
	fileIndex, err2 := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err2 != nil {
		return false
	}
	defer fileIndex.Close()

	//Symmary fajl
	fileName = "DataSSTable/Summary.bin"
	fileSummary, err3 := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err3 != nil {
		return false
	}
	defer fileSummary.Close()

	//Bloom Filter fajl
	fileBloom := "DataSSTable/BloomFilter.bin"
	// glavna petlja
	for i := 0; i < duzinaDataList; i++ {
		// dodali smo kljuc u bloomf
		AddKeyToBloomFilter(bloomFilter, dataList[i].GetKey())

		// serijaliacija podatka
		serializedData, err = SerializeDataType(dataList[i], true)
		if err != nil {
			return false
		}

		// upisujemo podatak u Data.bin fajl
		duzinaPodatka, err = file.Write(serializedData)
		if err != nil {
			return false
		}

		//Upis odgovarajucih vrednosti u Summary
		if i%M == 0 {
			indexData, err = SerializeIndexData(dataList[i].GetKey(), accIndex, compres)
			if err != nil {
				return false
			}
			fileSummary.Write(indexData)
		}
		//Upis odgovarajucih vrednosti u Index
		if i%N == 0 {
			indexData, err = SerializeIndexData(dataList[i].GetKey(), acc, compres)
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
	CreateMerkleTree(arrToMerkle, "DataSSTable/Merkle.bin")
	//Serijalizacija i upis bloom filtera
	err = bloomfilter.SerializeBloomFilter(bloomFilter, fileBloom)
	if err != nil {
		return false
	}
	fmt.Printf("%d\n", acc)
	return true
}
func ReadIndex(fileName string, key string, compres bool) bool {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		return false
	}
	defer file.Close()

	fileInfo, err := os.Stat(fileName)
	if err != nil {
		panic(err)
	}

	var currentRead int64

	currentRead = 0
	end := fileInfo.Size()
	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d\n", end)

	for currentRead != end {

		if compres == true {
			// read key size
			keySize, n := binary.Varint(bytesFile[currentRead:])
			//fmt.Printf("procitano: %d", n)
			currentRead += int64(n)

			//Read keys
			bytes := make([]byte, keySize)
			file.Seek(currentRead, 0)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}

			currentRead += int64(keySize)
			fmt.Printf("Kljuc : %s ", bytes)

			//Read offset
			offset, m := binary.Varint(bytesFile[currentRead:])
			currentRead += int64(m)
			fmt.Printf("Offset: %d \n", offset)
		} else {
			//read CRC
			bytes := make([]byte, 4)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += 4
			//fmt.Printf("%d", bytes)
			// read timestamp
			bytes = make([]byte, 16)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += 16
			//fmt.Printf("%d", bytes)
			// read tombstone
			bytes = make([]byte, 1)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			tomb := int(bytes[0])
			currentRead += 1
			//fmt.Printf("%d", bytes)
			// read key size
			bytes = make([]byte, 8)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += 8
			keySize := binary.BigEndian.Uint64(bytes)
			//fmt.Printf("%d", bytes)
			var valueSize uint64
			if tomb == 0 {
				// read value size
				bytes = make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				valueSize = binary.BigEndian.Uint64(bytes)
				currentRead += 8
				//fmt.Printf("%d", bytes)
			}
			// read key
			bytes = make([]byte, keySize)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += int64(keySize)
			fmt.Printf("Key: %s ", bytes)
			// read value
			if tomb == 0 {
				bytes = make([]byte, valueSize)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}

				fmt.Printf("Value: %s", bytes)
				currentRead += int64(valueSize)
			}
			fmt.Printf("\n")
		}

	}
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

func ReadSSTable(compres bool) bool {
	fileName := "DataSSTable/Data.bin"
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		return false
	}
	defer file.Close()

	fileInfo, err := os.Stat(fileName)
	if err != nil {
		panic(err)
	}

	var currentRead int64
	currentRead = 0
	end := fileInfo.Size()

	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}

	// var
	file.Seek(0, 0)
	for currentRead != end {
		//read CRC
		bytes := make([]byte, 4)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		currentRead += 4
		//fmt.Printf("%d", bytes)
		// read timestamp
		bytes = make([]byte, 16)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		currentRead += 16
		//fmt.Printf("%d", bytes)
		// read tombstone
		bytes = make([]byte, 1)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		tomb := int(bytes[0])
		currentRead += 1
		//fmt.Printf("%d", bytes)

		if compres {
			// read key size
			keySize, n := binary.Varint(bytesFile[currentRead:])
			currentRead += int64(n)
			// read value size
			var valueSize int64
			var m int
			if tomb == 0 {
				valueSize, m = binary.Varint(bytesFile[currentRead:])
				currentRead += int64(m)
			}
			// read key
			bytes := make([]byte, keySize)
			file.Seek(currentRead, 0)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Key: %s\n", bytes)
			currentRead += keySize
			// read value
			if tomb == 0 {
				bytes := make([]byte, valueSize)
				file.Seek(currentRead, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Value: %s\n", bytes)
				currentRead += valueSize
			}

		} else {
			bytes = make([]byte, 8)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += 8
			keySize := binary.BigEndian.Uint64(bytes)
			//fmt.Printf("%d", bytes)
			var valueSize uint64
			if tomb == 0 {
				// read value size
				bytes = make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				valueSize = binary.BigEndian.Uint64(bytes)
				currentRead += 8
				//fmt.Printf("%d", bytes)
			}
			// read key
			bytes = make([]byte, keySize)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += int64(keySize)
			fmt.Printf("Key: %s ", bytes)
			// read value
			if tomb == 0 {
				bytes = make([]byte, valueSize)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}

				fmt.Printf("Value: %s", bytes)
				currentRead += int64(valueSize)
			}
			fmt.Printf("\n")
		}

	}
	return true
}
