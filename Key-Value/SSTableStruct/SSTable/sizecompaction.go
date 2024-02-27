package SSTable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"log"
	"os"
	"sstable/bloomfilter/bloomfilter"
	"sstable/mem/memtable/datatype"
	"strconv"
	"time"
)

// N i M su nam redom razudjenost u index-u, i u summary-ju
func NewSSTableCompact(newFilePath string, compSSTable map[string][]int64, probability_bf float64, N, M, memtableLen int, compres1, compres2, oneFile bool) bool {

	var dictionary *map[string]int32
	// pomocne promenljive
	arrToMerkle := make([][]byte, 0)
	var serializedData, indexData []byte
	var duzinaPodatka, acc, accIndex int
	acc = 0
	accIndex = 0
	var err error
	bloomFilter := bloomfilter.CreateBloomFilter(uint64(memtableLen), probability_bf)

	// mapa za enkodirane vrednosti
	if compres2 {
		dictionary, err = DeserializationHashMap("EncodedKeys.bin")
		if err != nil {
			panic(err)
		}
	}

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

	GetOffsetStartEnd(&compSSTable)

	//var minData, maxData datatype.DataType

	// glavna petlja

	if compres2 {
		minData, maxData := GetGlobalSummaryMinMax(&compSSTable, compres1, compres2)
		indexData, err = SerializeIndexData(minData.GetKey(), accIndex, compres1, compres2, (*dictionary)[minData.GetKey()])
		if err != nil {
			return false
		}
		fileSummary.Write(indexData)
		indexData, err = SerializeIndexData(maxData.GetKey(), accIndex, compres1, compres2, (*dictionary)[maxData.GetKey()])
		if err != nil {
			return false
		}
		fileSummary.Write(indexData)
	} else {
		minData, maxData := GetGlobalSummaryMinMax(&compSSTable, compres1, compres2)
		indexData, err = SerializeIndexData(minData.GetKey(), accIndex, compres1, compres2, 0)
		if err != nil {
			return false
		}
		fileSummary.Write(indexData)
		indexData, err = SerializeIndexData(maxData.GetKey(), accIndex, compres1, compres2, 0)
		if err != nil {
			return false
		}
		fileSummary.Write(indexData)
	}

	i := 0
	for true {

		data, _ := getNextRecord(&compSSTable, compres1, compres2)
		if data.GetKey() == "" {
			break
		}

		if compres2 {
			_, exist := (*dictionary)[data.GetKey()]
			if !exist {
				(*dictionary)[data.GetKey()] = int32(len(*(dictionary)) + 1)
			}
		}

		// dodali smo kljuc u bloomf
		AddKeyToBloomFilter(bloomFilter, data.GetKey())

		if compres2 {
			// serijaliacija podatka
			serializedData, err = SerializeDataType(data, compres1, compres2, (*dictionary)[data.GetKey()])
			if err != nil {
				return false
			}
		} else {
			// serijaliacija podatka
			serializedData, err = SerializeDataType(data, compres1, compres2, 0)
			if err != nil {
				return false
			}
		}

		// upisujemo podatak u Data.bin fajl
		duzinaPodatka, err = file.Write(serializedData)
		if err != nil {
			return false
		}

		if compres2 {
			//Upis odgovarajucih vrednosti u Summary
			if (i+1)%M == 0 {
				indexData, err = SerializeIndexData(data.GetKey(), accIndex, compres1, compres2, (*dictionary)[data.GetKey()])
				if err != nil {
					return false
				}
				fileSummary.Write(indexData)
			}
			//Upis odgovarajucih vrednosti u Index
			if (i+1)%N == 0 {
				indexData, err = SerializeIndexData(data.GetKey(), acc, compres1, compres2, (*dictionary)[data.GetKey()])
				if err != nil {
					return false
				}
				fileIndex.Write(indexData)
				accIndex += len(indexData)

			}
		} else {
			//Upis odgovarajucih vrednosti u Summary
			if (i+1)%M == 0 {
				indexData, err = SerializeIndexData(data.GetKey(), accIndex, compres1, compres2, 0)
				if err != nil {
					return false
				}
				fileSummary.Write(indexData)
			}
			//Upis odgovarajucih vrednosti u Index
			if (i+1)%N == 0 {
				indexData, err = SerializeIndexData(data.GetKey(), acc, compres1, compres2, 0)
				if err != nil {
					return false
				}
				fileIndex.Write(indexData)
				accIndex += len(indexData)

			}
		}

		acc += duzinaPodatka

		// pomocni niz koji presludjemo za MerkleTree
		arrToMerkle = append(arrToMerkle, serializedData)

		i++

	}
	if compres2 {
		_, err = SerializeHashmap("EncodedKeys.bin", dictionary)
		if err != nil {
			panic(err)
		}
	}

	//Kreiranje i upis Merkle Stabla
	CreateMerkleTree(arrToMerkle, newFilePath+"/Merkle.bin")
	//Serijalizacija i upis bloom filtera
	err = bloomfilter.SaveToFile(bloomFilter, fileBloom)
	if err != nil {
		return false
	}

	// u slucaju da korisnik odabere sve u jedan fajl
	var serializedInOneFile []byte
	if oneFile {
		serializedInOneFile, err = WriteToOneFile(newFilePath+"/BloomFilter.bin", newFilePath+"/Summary.bin", newFilePath+"/Index.bin", newFilePath+"/Data.bin", newFilePath+"/Merkle.bin")
		if err != nil {
			panic(err)
		}

		file.Close()
		fileNameOneFile := newFilePath + "/SSTable.bin"
		e := os.Rename(newFilePath+"/Data.bin", fileNameOneFile)
		if e != nil {
			log.Fatal(e)
		}
		fileOne, err2 := os.OpenFile(fileNameOneFile, os.O_WRONLY|os.O_CREATE, 0666)
		if err2 != nil {
			panic(err2)
		}
		fileOne.Seek(0, 2)
		fileOne.Truncate(int64(len(serializedInOneFile)))
		fileOne.Write(serializedInOneFile)
		fileOne.Close()

		fileSummary.Close()
		fileIndex.Close()
		err = os.Remove(newFilePath + "/BloomFilter.bin")
		err = os.Remove(newFilePath + "/Summary.bin")
		err = os.Remove(newFilePath + "/Index.bin")
		err = os.Remove(newFilePath + "/Merkle.bin")
	}

	return true
}

func ReadDataCompact(filePath string, compres1, compres2 bool, offsetStart int64, elem int) (datatype.DataType, int64, bool) {
	oneFile := GetOneFile(filePath)
	var crc, tempCRC bytes.Buffer
	filename := "/Data.bin"
	if oneFile {
		filename = "/SSTable.bin"
	}
	filePath += filename

	var decodeMap *map[string]int32
	Data := datatype.CreateDataType("", []byte(""), time.Now())
	var size, sizeEnd int64
	//var fileNameHash string

	if compres2 {
		var err error
		// deserialization hashmap
		decodeMap, err = DeserializationHashMap("EncodedKeys.bin")
		if err != nil {
			panic(err)
		}

	}
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return *Data, 0, false
	}
	defer file.Close()

	file.Seek(0, 0)
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
		size, sizeEnd = PositionInSSTable(*file, elem)
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

	crc.Reset()
	tempCRC.Reset()

	//read CRC
	bytes := make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	crc.Write(bytes)
	currentRead += 4

	// read timestamp
	bytes = make([]byte, 16)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	nano := int64(binary.BigEndian.Uint64(bytes[8:]))
	timestamp = time.Unix(nano, 0)
	tempCRC.Write(bytes)

	currentRead += 16
	// read tombstone
	bytes = make([]byte, 1)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	tomb := int(bytes[0])
	tempCRC.Write(bytes)
	currentRead += 1

	if compres2 {
		if compres1 {
			// read key size - ne postoji

			// read value size
			var m int
			if tomb == 0 {
				valueSize, m = binary.Varint(bytesFile[currentRead:])
				next := currentRead + int64(m)
				tempCRC.Write(bytesFile[currentRead:next])
				currentRead += int64(m)
			}

			// read key
			key, k := binary.Varint(bytesFile[currentRead:])
			next := currentRead + int64(k)
			tempCRC.Write(bytesFile[currentRead:next])
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
				tempCRC.Write(bytes)
			}
			currentKey = GetKeyByValue(decodeMap, int32(key))

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
				tempCRC.Write(buff)
			}

			// read key
			buff := make([]byte, 4)
			_, err = file.Read(buff)
			if err != nil {
				panic(err)
			}
			currentRead += 4
			key := binary.BigEndian.Uint32(buff)
			tempCRC.Write(buff)
			currentKey = GetKeyByValue(decodeMap, int32(key))

			// read value
			if tomb == 0 {
				buff = make([]byte, valueSize)
				_, err = file.Read(buff)
				if err != nil {
					panic(err)
				}
				currentRead += int64(valueSize)
				currentValue = buff
				tempCRC.Write(buff)
			}

		}
	} else {
		if compres1 {
			// read key size
			keySize, n := binary.Varint(bytesFile[currentRead:])
			next := currentRead + int64(n)
			tempCRC.Write(bytesFile[currentRead:next])
			currentRead += int64(n)
			// read value size
			var m int
			if tomb == 0 {
				valueSize, m = binary.Varint(bytesFile[currentRead:])
				next = currentRead + int64(n)
				tempCRC.Write(bytesFile[currentRead:next])
				currentRead += int64(m)
			}
			// read key
			bytes = make([]byte, keySize)
			file.Seek(currentRead+offsetStart, 0)

			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			tempCRC.Write(bytes)
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
				tempCRC.Write(bytes)
			}

		} else {
			bytes = make([]byte, 8)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += 8
			tempCRC.Write(bytes)
			keySize := binary.BigEndian.Uint64(bytes)
			if tomb == 0 {
				// read value size
				bytes = make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				tempCRC.Write(bytes)

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
			tempCRC.Write(bytes)
			// read value
			if tomb == 0 {
				bytes = make([]byte, valueSize)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}

				currentRead += int64(valueSize)
				currentValue = bytes
				tempCRC.Write(bytes)
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

	if crc32.ChecksumIEEE(tempCRC.Bytes()) != binary.BigEndian.Uint32(crc.Bytes()) {
		Data.SetKey("")
	}
	return *Data, currentRead, true
}

func setStartEndOffset(filePath string, numberSSTable int) ([]int64, []int64, bool) {
	oneFile := GetOneFile(filePath)
	endOffsetList := make([]int64, numberSSTable)
	startOffsetList := make([]int64, numberSSTable)
	fileName := "/Data.bin"
	var elem int
	elem = 5
	if oneFile {
		fileName = "/SSTable.bin"
		for i := 1; i <= numberSSTable; i++ {

			file, err := os.OpenFile(filePath+"/sstable"+strconv.Itoa(i)+fileName, os.O_RDONLY, 0666)
			if err != nil {
				return startOffsetList, endOffsetList, false
			}
			defer file.Close()

			start, end := PositionInSSTable(*file, elem)
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

func getNextRecord(compSSTable *map[string][]int64, compres1, compres2 bool) (datatype.DataType, bool) {

	var elem int
	elem = 5
	var data datatype.DataType
	data.SetKey("")
	same := 0

	for path, _ := range *compSSTable {
		if (*compSSTable)[path][0] == (*compSSTable)[path][1] {
			same += 1
			continue
		}

		currentData, _, err1 := ReadDataCompact(path, compres1, compres2, (*compSSTable)[path][0], elem)
		if err1 != true {
			return data, false
		}
		if data.GetKey() == "" {
			data = currentData
		} else if currentData.GetKey() == data.GetKey() {
			if data.GetChangeTime().Before(currentData.GetChangeTime()) {
				data = currentData
			}
		} else if currentData.GetKey() < data.GetKey() {
			data = currentData
		}
	}

	if same == len(*compSSTable) {
		data.SetKey("")
		return data, true
	}
	for path, _ := range *compSSTable {
		if (*compSSTable)[path][0] == (*compSSTable)[path][1] {
			same += 1
			continue
		}
		file, err := os.OpenFile(path, os.O_RDONLY, 0666)
		if err != nil {
			return data, false
		}
		defer file.Close()

		currentData, read, err1 := ReadDataCompact(path, compres1, compres2, (*compSSTable)[path][0], elem)
		if err1 != true {
			return data, false
		}
		if currentData.GetKey() == data.GetKey() {
			(*compSSTable)[path][0] += read
		}
	}
	//if data.IsDeleted() == true {
	//	return data, false
	//}
	return data, true
}
