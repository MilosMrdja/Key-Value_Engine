package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sstable/bloomfilter/bloomfilter"
	"sstable/mem/memtable/datatype"
	"strconv"
	"time"
)

// N i M su nam redom razudjenost u index-u, i u summary-ju
func NewSSTableCompact(newFilePath string, numberSSTable int, oldFilePath string, N, M, memtableLen int, compres, oneFile bool) bool {

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
	startOffsetList, endOffsetList, greska := setStartEndOffset(oldFilePath, numberSSTable, oneFile)
	if greska != true {
		return false
	}

	i := 0
	for true {

		data, greska := getNextRecord(oldFilePath, startOffsetList, endOffsetList, compres, oneFile)
		fmt.Printf("Kljuc: %s\n", data.GetKey())
		if greska == false {
			return false
		}
		if data.GetKey() == "" {
			break
		}

		// dodali smo kljuc u bloomf
		AddKeyToBloomFilter(bloomFilter, data.GetKey())

		// serijaliacija podatka
		serializedData, err = SerializeDataType(data, compres)
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
			indexData, err = SerializeIndexData(data.GetKey(), accIndex, compres)
			if err != nil {
				return false
			}
			fileSummary.Write(indexData)
		}
		//Upis odgovarajucih vrednosti u Index
		if (i+1)%N == 0 {
			indexData, err = SerializeIndexData(data.GetKey(), acc, compres)
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
	//Kreiranje i upis Merkle Stabla
	CreateMerkleTree(arrToMerkle, newFilePath+"/Merkle.bin")
	//Serijalizacija i upis bloom filtera
	err = bloomfilter.SerializeBloomFilter(bloomFilter, fileBloom)
	if err != nil {
		return false
	}
	//fmt.Printf("%d\n", acc)

	// u slucaju da korisnik odabere sve u jedan fajl

	if oneFile {
		serializedInOneFile, err := WriteToOneFile(newFilePath+"/BloomFilter.bin", newFilePath+"/Summary.bin", newFilePath+"/Index.bin", newFilePath+"/Data.bin", newFilePath+"/Merkle.bin")
		if err != nil {
			panic(err)
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
func ReadDataCompact(filePath string, compres bool, offsetStart int64, oneFile bool, elem int) (datatype.DataType, int64, bool) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	Data := datatype.CreateDataType("", []byte(""))
	if err != nil {
		return *Data, 0, false
	}
	defer file.Close()

	var result bytes.Buffer
	var currentRead int64
	var timestamp time.Time
	currentRead = 0
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		panic(err)
	}
	end := fileInfo.Size()
	var size, sizeEnd int64
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
	//fmt.Printf("%d", bytes)
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
	//fmt.Printf("%d", bytes)
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
		bytes = make([]byte, keySize)
		file.Seek(currentRead+offsetStart, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		//fmt.Printf("Key: %s\n", bytes)
		currentKey := string(bytes)
		currentRead += keySize
		// read value
		if tomb == 0 {
			bytes = make([]byte, valueSize)
			file.Seek(currentRead+offsetStart, 0)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			//fmt.Printf("Value: %s\n", bytes)
			currentRead += valueSize
		}
		Data.SetKey(currentKey)
		if tomb == 0 {
			Data.SetDelete(false)
			Data.SetData(bytes)
		} else {
			Data.SetDelete(true)
		}
	} else {
		bytes = make([]byte, 8)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		currentRead += 8
		err = binary.Write(&result, binary.BigEndian, bytes)
		if err != nil {
			return *Data, 0, false
		}
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
			err = binary.Write(&result, binary.BigEndian, bytes)
			if err != nil {
				return *Data, 0, false
			}
			currentRead += 8
			//fmt.Printf("%d", bytes)
		}
		// read key
		bytes = make([]byte, keySize)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		err = binary.Write(&result, binary.BigEndian, bytes)
		if err != nil {
			return *Data, 0, false
		}
		currentKey := string(bytes)
		currentRead += int64(keySize)
		//fmt.Printf("Key: %s ", bytes)
		// read value
		// ako nije obrisan podatak, cita se njegova vrednost
		if tomb == 0 {
			bytes = make([]byte, valueSize)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}

			//fmt.Printf("Value: %s", bytes)
			currentRead += int64(valueSize)
			err = binary.Write(&result, binary.BigEndian, bytes)
			if err != nil {
				return *Data, 0, false
			}
			//ako je trazeni podatak obrisan, zaustavlja se trazenje
		}
		Data.SetKey(currentKey)
		if tomb == 0 {
			Data.SetDelete(false)
			Data.SetData(bytes)
		} else {
			Data.SetDelete(true)
		}

	}
	Data.SetChangeTime(timestamp)

	//fmt.Printf("\n")
	return *Data, currentRead, true
}

func setStartEndOffset(filePath string, numberSSTable int, oneFile bool) ([]int64, []int64, bool) {

	endOffsetList := make([]int64, numberSSTable)
	startOffsetList := make([]int64, numberSSTable)
	fileName := "/Data.bin"
	if oneFile {
		fileName = "/SSTable.bin"
		for i := 1; i <= numberSSTable; i++ {

			file, err := os.OpenFile(filePath+"/sstable"+strconv.Itoa(i)+fileName, os.O_RDONLY, 0666)
			if err != nil {
				return startOffsetList, endOffsetList, false
			}
			defer file.Close()

			start, end := positionInSSTable(*file, 3)
			endOffsetList[i-1] = end - start
			startOffsetList[i-1] = 0
		}
	} else {
		for i := 1; i <= numberSSTable; i++ {

			fileInfo, err := os.Stat(filePath + fileName)
			if err != nil {
				panic(err)
			}
			endOffsetList[i-1] = fileInfo.Size()
			startOffsetList[i-1] = 0
		}
	}

	return startOffsetList, endOffsetList, true
}

func getNextRecord(filePath string, startOffsetList, endOffsetList []int64, compres, oneFile bool) (datatype.DataType, bool) {
	var data datatype.DataType
	fileName := "/Data.bin"
	if oneFile {
		fileName = "/SSTable.bin"
	}
	data.SetKey("")
	same := 0
	for i := 1; i <= len(startOffsetList); i++ {
		if startOffsetList[i-1] == endOffsetList[i-1] {
			same += 1
			continue
		}
		file, err := os.OpenFile(filePath+"/sstable"+strconv.Itoa(i)+fileName, os.O_RDONLY, 0666)
		if err != nil {
			return data, false
		}
		defer file.Close()

		currentData, read, err1 := ReadDataCompact(filePath+fileName, compres, startOffsetList[i-1], oneFile, 3)
		if err1 != true {
			return data, false
		}
		startOffsetList[i-1] += read
		if data.GetKey() == "" {
			data = currentData
		} else if currentData.GetKey() < data.GetKey() {
			data = currentData
		} else if currentData.GetKey() == data.GetKey() {
			data = currentData
		}

	}

	if same == len(startOffsetList) {
		data.SetKey("")
		return data, true
	}
	return data, true
}
