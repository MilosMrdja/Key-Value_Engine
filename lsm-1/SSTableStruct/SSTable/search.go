package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sstable/bloomfilter/bloomfilter"
	"sstable/mem/memtable/datatype"
	"time"
)

func GetData(filePath string, key string, compres bool, oneFile bool) (datatype.DataType, bool) {
	var data datatype.DataType
	if oneFile {
		fileName := filePath + "/SSTable.bin"
		bloomFilter, err2 := bloomfilter.DeserializeBloomFilter(fileName, true)
		if err2 != nil {
			return data, false
		}
		isInFile := bloomFilter.Get([]byte(key))
		if isInFile == true {
			offsetStart, offsetEnd, err3 := GetOffset(fileName, key, compres, 0, 0, oneFile, 1)
			if err3 == false {
				return data, false
			}
			offsetStart, offsetEnd, err3 = GetOffset(fileName, key, compres, offsetStart, offsetEnd, oneFile, 2)
			if err3 == false {
				return data, false
			}
			fmt.Printf("%d\n", offsetStart)
			data, err3 = ReadData(fileName, compres, offsetStart, offsetEnd, key, oneFile, 3)
			if err3 == false {
				return data, false
			}

			return data, true
		}
	} else {
		bloomFilter, err2 := bloomfilter.DeserializeBloomFilter(filePath+"/BloomFilter.bin", false)
		if err2 != nil {
			return data, false
		}
		isInFile := bloomFilter.Get([]byte(key))
		if isInFile == true {
			offsetStart, offsetEnd, err3 := GetOffset(filePath+"/Summary.bin", key, compres, 0, 0, oneFile, 1)
			if err3 == false {
				return data, false
			}
			offsetStart, offsetEnd, err3 = GetOffset(filePath+"/Index.bin", key, compres, offsetStart, offsetEnd, oneFile, 2)
			if err3 == false {
				return data, false
			}
			fmt.Printf("%d\n", offsetStart)
			data, err3 = ReadData(filePath+"/Data.bin", compres, offsetStart, offsetEnd, key, oneFile, 3)
			if err3 == false {
				return data, false
			}

			return data, true
		}

	}

	return data, false
}

func ReadData(filePath string, compres bool, offsetStart, offsetEnd int64, key string, oneFile bool, elem int) (datatype.DataType, bool) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	Data := datatype.CreateDataType("", []byte(""))
	if err != nil {
		return *Data, false
	}
	defer file.Close()
	file.Seek(int64(offsetStart), 0)

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
		offsetEnd = (sizeEnd) - (offsetEnd - offsetStart)
		end = sizeEnd - size
		if err != nil {
			return *Data, false
		}
	}
	if offsetEnd == 0 {
		offsetEnd = fileInfo.Size()
	}

	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(offsetStart, 0)

	for offsetStart <= offsetEnd {
		//read CRC
		bytes := make([]byte, 4)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		currentRead += 4
		err = binary.Write(&result, binary.BigEndian, bytes)
		if err != nil {
			return *Data, false
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
			return *Data, false
		}
		err = binary.Write(&result, binary.BigEndian, bytes)
		if err != nil {
			return *Data, false
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
			return *Data, false
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
			file.Seek(currentRead+size, 0)
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
				file.Seek(currentRead+size, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				//fmt.Printf("Value: %s\n", bytes)
				currentRead += valueSize
			} else if currentKey == key {
				result.Reset()
				return *Data, false
			}
			if currentKey == key {
				Data = datatype.CreateDataType(key, bytes)
				Data.SetDelete(false)
				Data.SetChangeTime(timestamp)
				return *Data, true
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
				return *Data, false
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
					return *Data, false
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
				return *Data, false
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
					return *Data, false
				}
				//ako je trazeni podatak obrisan, zaustavlja se trazenje
			} else if currentKey == key {
				result.Reset()
				return *Data, false
			}
			//ako podatak postoji i on je trazeni, vraca se
			if currentKey == key {
				Data = datatype.CreateDataType(key, bytes)
				Data.SetDelete(false)
				Data.SetChangeTime(timestamp)
				return *Data, true
			}
			result.Reset()
		}
		offsetStart += currentRead
		//fmt.Printf("\n")
	}
	return *Data, true
}

func GetOffset(filePath, key string, compres bool, offsetStart, offsetEnd int64, oneFile bool, elem int) (int64, int64, bool) {

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return 0, 0, false
	}
	defer file.Close()

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		panic(err)
	}
	var size, sizeEnd int64
	end := fileInfo.Size()
	if oneFile {

		size, sizeEnd = positionInSSTable(*file, elem)
		offsetEnd = sizeEnd - offsetEnd
		offsetStart += size

		end = sizeEnd - size
		if err != nil {
			return 0, 0, false
		}
	}
	if offsetEnd == 0 {
		offsetEnd = fileInfo.Size()
	}
	var currentRead int64

	currentRead = 0

	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(offsetStart, 0)
	var currentOffset int64
	currentOffset = 0

	for offsetStart <= offsetEnd {

		if compres == true {
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
			currentKey := string(bytes)
			currentRead += int64(keySize)
			//fmt.Printf("Kljuc : %s ", currentKey)

			if currentKey > key {
				offsetEnd = int64(binary.BigEndian.Uint64(bytes))
				return currentOffset, offsetEnd, true
			}
			//fmt.Printf("Offset: %d \n", binary.BigEndian.Uint32(bytes))
			if currentKey == key {
				//Read offset
				off, m := binary.Varint(bytesFile[currentRead:])
				currentOffset = off
				currentRead += int64(m)
				//fmt.Printf("Offset: %d \n", currentOffset)
				return currentOffset, currentOffset, true
			}

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
			//fmt.Printf("Kljuc : %s ", bytes)

			currentKey := string(bytes)
			currentRead += int64(keySize)
			//fmt.Printf("Kljuc : %s ", currentKey)

			//Read offset
			bytes = make([]byte, 8)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			currentRead += 8
			if currentKey > key {
				offsetEnd = int64(binary.BigEndian.Uint64(bytes))
				return currentOffset, offsetEnd, true
			}
			//fmt.Printf("Offset: %d \n", binary.BigEndian.Uint32(bytes))
			currentOffset = int64(binary.BigEndian.Uint64(bytes))
			if currentKey == key {
				return currentOffset, currentOffset, true
			}

		}
		offsetStart += currentRead

	}
	return currentOffset, 0, true
}
