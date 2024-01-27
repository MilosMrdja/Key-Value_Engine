package SSTable

import (
	"encoding/binary"
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
	// brojac za mapu i mapa za enkodirane vrednost

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

	// u slucaju da korisnik odabere sve u jedan fajl

	if oneFile {
		serializedInOneFile, err := WriteToOneFile(fileName+"/BloomFilter.bin", fileName+"/Summary.bin", fileName+"/Index.bin", fileName+"/Data.bin", fileName+"/Merkle.bin")
		if err != nil {
			panic(err)
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

	bs, _ := json.Marshal(dictionary)
	fmt.Println(string(bs))
	return true
}

func ReadIndex(fileName string, key string, compress1, compress2 bool, elem int, oneFile bool) bool {
	if oneFile {
		fileName = fileName + "/SSTable.bin"
	}
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

	// var

	var size, sizeEnd int64
	if oneFile {
		size, sizeEnd = positionInSSTable(*file, elem)
		end = sizeEnd - size
		_, err1 := file.Seek(size, 0)
		if err1 != nil {
			return false
		}
	} else {
		_, err = file.Seek(0, 0)
		if err != nil {
			return false
		}
	}
	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(size, 0)
	fmt.Printf("Velicina: %d\n", (end))
	var keySize int
	for currentRead != end {

		if compress2 {
			if compress1 {
				// read key size
				byt := make([]byte, 1)
				file.Seek(currentRead, 0)
				_, err = file.Read(byt)
				if err != nil {
					panic(err)
				}
				keySize = int(byt[0])
				fmt.Println(keySize)
				currentRead += 1
				//read key
				bytes := make([]byte, keySize)
				file.Seek(currentRead+size, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Key: %s ", bytes)
				currentRead += int64(keySize)
				// read offset
				offset, m := binary.Varint(bytesFile[currentRead:])
				currentRead += int64(m)
				fmt.Printf("Offset: %d \n", offset)
			}
		} else {
			if compress1 == true {
				// read key size
				keySize, n := binary.Varint(bytesFile[currentRead:])
				//fmt.Printf("procitano: %d", n)
				currentRead += int64(n)

				//Read keys
				bytes := make([]byte, keySize)
				file.Seek(currentRead+size, 0)
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
				// read key size
				bytes := make([]byte, 4)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += 4
				keySize := binary.BigEndian.Uint32(bytes)

				//Read key
				bytes = make([]byte, keySize)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += int64(keySize)
				fmt.Printf("Kljuc : %s ", bytes)

				//Read offset
				bytes = make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += 8
				fmt.Printf("Offset: %d \n", binary.BigEndian.Uint64(bytes))
			}
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

func ReadSSTable(filePath string, compress1, compress2, oneFile bool) bool {

	fileName := filePath + "/Data.bin"
	if oneFile {
		fileName = filePath + "/SSTable.bin"
	}
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

	// var

	var size, sizeEnd int64
	if oneFile {
		size, sizeEnd = positionInSSTable(*file, 3)
		end = sizeEnd - size
		_, err1 := file.Seek(size, 0)
		if err1 != nil {
			return false
		}
	} else {
		_, err = file.Seek(0, 0)
		if err != nil {
			return false
		}
	}
	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(size, 0)
	fmt.Printf("Velicina: %d\n", (end))
	for currentRead != end {
		//read CRC
		bytes := make([]byte, 4)
		file.Seek(currentRead, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		currentRead += 4
		//fmt.Printf("%d", bytes)

		// read timestamp
		bytes = make([]byte, 16)
		file.Seek(currentRead, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		currentRead += 16
		//fmt.Printf("%d", bytes)

		// read tombstone
		bytes = make([]byte, 1)
		file.Seek(currentRead, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		tomb := int(bytes[0])
		currentRead += 1
		//fmt.Printf("%d", bytes)

		if compress2 {
			if compress1 {
				// read key size
				byt := make([]byte, 1)
				file.Seek(currentRead, 0)
				_, err = file.Read(byt)
				if err != nil {
					panic(err)
				}
				keySize := int(byt[0])
				fmt.Println(keySize)
				currentRead += 1
				// read value size
				var valueSize int64
				var m int
				if tomb == 0 {
					valueSize, m = binary.Varint(bytesFile[currentRead:])

					currentRead += int64(m)
				}

				// read key
				key, _ := binary.Varint(bytesFile[currentRead:])
				fmt.Printf("Key: %d ", key)
				currentRead += int64(keySize)
				// read value
				if tomb == 0 {
					bytes = make([]byte, valueSize)
					file.Seek(currentRead, 0)
					_, err = file.Read(bytes)
					if err != nil {
						panic(err)
					}
					fmt.Printf("Value: %s", bytes)
					currentRead += valueSize
				}
			}
		} else {
			if compress1 {
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
				bytes = make([]byte, keySize)
				file.Seek(currentRead+size, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Key: %s ", bytes)
				currentRead += keySize
				// read value
				if tomb == 0 {
					bytes = make([]byte, valueSize)
					file.Seek(currentRead+size, 0)
					_, err = file.Read(bytes)
					if err != nil {
						panic(err)
					}
					fmt.Printf("Value: %s", bytes)
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
				} // read key
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

			}

		}
		fmt.Printf("\n")

	}
	return true
}
