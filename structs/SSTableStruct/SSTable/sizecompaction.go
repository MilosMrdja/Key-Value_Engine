package SSTable

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"sstable/bloomfilter/bloomfilter"
	"sstable/mem/memtable/datatype"
	"strconv"
	"time"
)

// N i M su nam redom razudjenost u index-u, i u summary-ju
func NewSSTableCompact(newFilePath string, numberSSTable int, oldFilePath string, N, M, memtableLen int, compres1, compres2, oneFile bool) bool {

	// pomocne promenljive
	arrToMerkle := make([][]byte, 0)
	var serializedData, indexData []byte
	var duzinaPodatka, acc, accIndex, duzinaDataList int
	acc = 0
	accIndex = 0
	duzinaDataList = numberSSTable * memtableLen
	var err error
	bloomFilter := bloomfilter.CreateBloomFilter(duzinaDataList)

	//Data fajl
	fileName := newFilePath + "/Data.bin"
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return false
	}
	defer file.Close()

	//Index fajl
	fileName = newFilePath + "/Index.bin"
	fileIndex, err2 := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err2 != nil {
		return false
	}
	defer fileIndex.Close()

	//Symmary fajl
	fileName = newFilePath + "/Summary.bin"
	fileSummary, err3 := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err3 != nil {
		return false
	}
	defer fileSummary.Close()

	//Bloom Filter fajl
	fileBloom := newFilePath + "/BloomFilter.bin"
	// glavna petlja

	//dobavljanje sledeceg najmanjeg elementa
	startOffsetList, endOffsetList, greska := setStartEndOffset(oldFilePath, numberSSTable, compres2, oneFile)
	if greska != true {
		return false
	}

	i := 0
	dictionary := make(map[string]int32)
	for true {

		data, greska := getNextRecord(oldFilePath, startOffsetList, endOffsetList, compres1, compres2, oneFile)
		if greska == false && data.GetKey() != "" {
			continue
		}
		if data.GetKey() == "" {
			break
		}

		dictionary[data.GetKey()] = int32(i)

		// dodali smo kljuc u bloomf
		AddKeyToBloomFilter(bloomFilter, data.GetKey())

		// serijaliacija podatka
		serializedData, err = SerializeDataType(data, compres1, compres2, int32(i))
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
			indexData, err = SerializeIndexData(data.GetKey(), accIndex, compres1, compres2, int32(i))
			if err != nil {
				return false
			}
			fileSummary.Write(indexData)
		}
		//Upis odgovarajucih vrednosti u Index
		if (i+1)%N == 0 {
			indexData, err = SerializeIndexData(data.GetKey(), acc, compres1, compres2, int32(i))
			if err != nil {
				return false
			}
			fileIndex.Write(indexData)
			accIndex += len(indexData)

		}

		acc += duzinaPodatka

		// pomocni niz koji presludjemo za MerkleTree
		arrToMerkle = append(arrToMerkle, serializedData)

		i++

	}
	hashFileName := newFilePath + "/HashMap.bin"
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
	//Kreiranje i upis Merkle Stabla
	CreateMerkleTree(arrToMerkle, newFilePath+"/Merkle.bin")
	//Serijalizacija i upis bloom filtera
	err = bloomfilter.SerializeBloomFilter(bloomFilter, fileBloom)
	if err != nil {
		return false
	}

	// u slucaju da korisnik odabere sve u jedan fajl
	var serializedInOneFile []byte
	if oneFile {
		if compres2 {
			serializedInOneFile, err = WriteToOneFile(newFilePath+"/BloomFilter.bin", newFilePath+"/HashMap.bin", newFilePath+"/Summary.bin", newFilePath+"/Index.bin", newFilePath+"/Data.bin", newFilePath+"/Merkle.bin")
			if err != nil {
				panic(err)
			}
		} else {
			serializedInOneFile, err = WriteToOneFile(newFilePath+"/BloomFilter.bin", "", newFilePath+"/Summary.bin", newFilePath+"/Index.bin", newFilePath+"/Data.bin", newFilePath+"/Merkle.bin")
			if err != nil {
				panic(err)
			}
		}

		// One file
		fileNameOneFile := newFilePath + "/SSTable.bin"
		fileOne, err2 := os.OpenFile(fileNameOneFile, os.O_WRONLY|os.O_CREATE, 0666)
		if err2 != nil {
			fmt.Println("Adsas")
		}
		defer fileOne.Close()
		fileOne.Write(serializedInOneFile)
	}

	return true
}
func ReadDataCompact(filePath string, compres1, compres2 bool, offsetStart int64, oneFile bool, elem int) (datatype.DataType, int64, bool) {
	var decodeMap map[string]int32
	Data := datatype.CreateDataType("", []byte(""))
	var size, sizeEnd int64
	var fileNameHash string
	if oneFile {
		fileNameHash = filePath + "/SSTable.bin"
	} else {
		fileNameHash = filePath + "/HashMap.bin"
	}

	if compres2 {
		elem = 4
		file, err := os.OpenFile(fileNameHash, os.O_RDONLY, 0666)
		if err != nil {
			return *Data, 0, false
		}
		defer file.Close()
		if oneFile {
			//ako je hash mapa u jednom fajlu
			size, sizeEnd = positionInSSTable(*file, 1)
			file.Seek(size, 0)
			bbb := make([]byte, sizeEnd-size)
			bb := bytes.NewBuffer(bbb)
			_, err = file.Read(bbb)
			if err != nil {
				panic(err)
			}
			d := gob.NewDecoder(bb)
			err = d.Decode(&decodeMap)
			if err != nil {
				panic(err)
			}
		} else {
			fileHash, err := os.OpenFile(fileNameHash, os.O_RDONLY, 0666)
			if err != nil {
				panic(err)
			}
			defer file.Close()
			fileInfoHash, err := os.Stat(fileNameHash)
			if err != nil {
				panic(err)
			}

			end := fileInfoHash.Size()

			bbb := make([]byte, end)
			bb := bytes.NewBuffer(bbb)
			_, err = fileHash.Read(bbb)
			if err != nil {
				panic(err)
			}
			d := gob.NewDecoder(bb)
			err = d.Decode(&decodeMap)
			if err != nil {
				panic(err)
			}
		}

	}
	if oneFile {
		filePath += "/SSTable.bin"
	} else {
		filePath += "/Data.bin"
	}
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return *Data, 0, false
	}
	defer file.Close()

	file.Seek(0, 0)
	var result bytes.Buffer
	var currentRead int64
	var currentKey string
	var currentValue []byte
	var valueSize int64
	var timestamp time.Time
	currentRead = 0
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		panic(err)
	}

	end := fileInfo.Size()

	if oneFile {
		size, sizeEnd = positionInSSTable(*file, elem)
		offsetStart += size
		end = sizeEnd - size
		if err != nil {
			return *Data, 0, false
		}
	}
	file.Seek(offsetStart, 0)
	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(offsetStart, 0)

	//read CRC
	bytes := make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	currentRead += 4
	err = binary.Write(&result, binary.BigEndian, bytes)
	if err != nil {
		return *Data, 0, false
	}
	// read timestamp
	bytes = make([]byte, 16)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	nano := int64(binary.BigEndian.Uint64(bytes[8:]))
	timestamp = time.Unix(nano, 0)
	if err != nil {
		return *Data, 0, false
	}
	err = binary.Write(&result, binary.BigEndian, bytes)
	if err != nil {
		return *Data, 0, false
	}
	currentRead += 16
	// read tombstone
	bytes = make([]byte, 1)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	tomb := int(bytes[0])
	err = binary.Write(&result, binary.BigEndian, bytes)
	if err != nil {
		return *Data, 0, false
	}
	currentRead += 1

	if compres2 {
		if compres1 {
			// read key size - ne postoji

			// read value size
			var m int
			if tomb == 0 {
				valueSize, m = binary.Varint(bytesFile[currentRead:])
				currentRead += int64(m)
			}

			// read key
			key, k := binary.Varint(bytesFile[currentRead:])
			currentRead += int64(k)
			// read value
			if tomb == 0 {
				bytes = make([]byte, valueSize)
				file.Seek(currentRead+offsetStart, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += int64(valueSize)
				currentValue = bytes
			}
			currentKey = GetKeyByValue(&decodeMap, int32(key))

		} else {
			// read key size - znamo da je 4 bajta maks

			// read value size
			if tomb == 0 {

				buff := make([]byte, 8)
				file.Seek(currentRead+offsetStart, 0)
				_, err = file.Read(buff)
				if err != nil {
					panic(err)
				}
				currentRead += 8
				valueSize = int64(binary.BigEndian.Uint64(buff))
			}

			// read key
			buff := make([]byte, 4)
			_, err = file.Read(buff)
			if err != nil {
				panic(err)
			}
			currentRead += 4
			key := binary.BigEndian.Uint32(buff)
			currentKey = GetKeyByValue(&decodeMap, int32(key))
			// read value
			if tomb == 0 {
				buff = make([]byte, valueSize)
				_, err = file.Read(buff)
				if err != nil {
					panic(err)
				}
				currentRead += int64(valueSize)
				currentValue = buff
			}

		}
	} else {
		if compres1 {
			// read key size
			keySize, n := binary.Varint(bytesFile[currentRead:])
			currentRead += int64(n)
			// read value size
			var m int
			if tomb == 0 {
				valueSize, m = binary.Varint(bytesFile[currentRead:])
				currentRead += int64(m)
			}
			// read key
			bytes = make([]byte, keySize)
			file.Seek(currentRead+offsetStart, 0)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += keySize
			// read value
			currentKey = string(bytes)
			if tomb == 0 {
				bytes = make([]byte, valueSize)
				file.Seek(currentRead+offsetStart, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += valueSize
				currentValue = bytes
			}

		} else {
			bytes = make([]byte, 8)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += 8
			keySize := binary.BigEndian.Uint64(bytes)
			if tomb == 0 {
				// read value size
				bytes = make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				valueSize = int64(binary.BigEndian.Uint64(bytes))
				currentRead += 8
			} // read key
			bytes = make([]byte, keySize)

			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += int64(keySize)
			currentKey = string(bytes)
			// read value
			if tomb == 0 {
				bytes = make([]byte, valueSize)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}

				currentRead += int64(valueSize)
				currentValue = bytes
			}

		}
	}
	Data.SetKey(currentKey)
	if tomb == 0 {
		Data.SetDelete(false)
		Data.SetData(currentValue)
	} else {
		Data.SetDelete(true)
	}
	Data.SetChangeTime(timestamp)

	//fmt.Printf("\n")
	return *Data, currentRead, true
}

func setStartEndOffset(filePath string, numberSSTable int, compres2, oneFile bool) ([]int64, []int64, bool) {

	endOffsetList := make([]int64, numberSSTable)
	startOffsetList := make([]int64, numberSSTable)
	fileName := "/Data.bin"
	var elem int
	if compres2 {
		elem = 4
	} else {
		elem = 3
	}
	if oneFile {
		fileName = "/SSTable.bin"
		for i := 1; i <= numberSSTable; i++ {

			file, err := os.OpenFile(filePath+"/sstable"+strconv.Itoa(i)+fileName, os.O_RDONLY, 0666)
			if err != nil {
				return startOffsetList, endOffsetList, false
			}
			defer file.Close()

			start, end := positionInSSTable(*file, elem)
			endOffsetList[i-1] = end - start
			startOffsetList[i-1] = 0
		}
	} else {
		for i := 1; i <= numberSSTable; i++ {

			fileInfo, err := os.Stat(filePath + "/sstable" + strconv.Itoa(i) + fileName)
			if err != nil {
				panic(err)
			}
			endOffsetList[i-1] = fileInfo.Size()
			startOffsetList[i-1] = 0
		}
	}

	return startOffsetList, endOffsetList, true
}

func getNextRecord(filePath string, startOffsetList, endOffsetList []int64, compres1, compres2, oneFile bool) (datatype.DataType, bool) {
	var elem int
	if compres2 {
		elem = 4
	} else {
		elem = 3
	}
	var data datatype.DataType
	data.SetKey("")
	same := 0
	for i := 1; i <= len(startOffsetList); i++ {
		if startOffsetList[i-1] == endOffsetList[i-1] {
			same += 1
			continue
		}
		file, err := os.OpenFile(filePath+"/sstable"+strconv.Itoa(i), os.O_RDONLY, 0666)
		if err != nil {
			return data, false
		}
		defer file.Close()

		currentData, _, err1 := ReadDataCompact(filePath+"/sstable"+strconv.Itoa(i), compres1, compres2, startOffsetList[i-1], oneFile, elem)
		if err1 != true {
			return data, false
		}
		if data.GetKey() == "" {
			data = currentData
		} else if currentData.GetKey() == data.GetKey() {
			if data.GetChangeTime().Before(currentData.GetChangeTime()) {
				currentData = data
			}
		} else if currentData.GetKey() < data.GetKey() {
			data = currentData
		}
	}

	if same == len(startOffsetList) {
		data.SetKey("")
		return data, true
	}
	for i := 1; i <= len(startOffsetList); i++ {
		if startOffsetList[i-1] == endOffsetList[i-1] {
			same += 1
			continue
		}
		file, err := os.OpenFile(filePath+"/sstable"+strconv.Itoa(i), os.O_RDONLY, 0666)
		if err != nil {
			return data, false
		}
		defer file.Close()

		currentData, read, err1 := ReadDataCompact(filePath+"/sstable"+strconv.Itoa(i), compres1, compres2, startOffsetList[i-1], oneFile, 3)
		if err1 != true {
			return data, false
		}
		if currentData.GetKey() == data.GetKey() {
			startOffsetList[i-1] += read
		}
	}
	if data.IsDeleted() == true {
		return data, false
	}
	return data, true
}
