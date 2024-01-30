package SSTable

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"sstable/bloomfilter/bloomfilter"
	"sstable/mem/memtable/datatype"
	"time"
)

func GetData(filePath string, key string, compress1, compress2 bool, oneFile bool) (datatype.DataType, bool) {
	var data datatype.DataType
	var hashMap map[string]int32
	if oneFile {
		fileName := filePath + "/SSTable.bin"
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
		if err != nil {
			return data, false
		}
		size, _ := positionInSSTable(*file, 1)
		file.Seek(size, 0)
		bloomFilter, err2 := bloomfilter.DeserializeBloomFilter(file)
		if err2 != nil {
			return data, false
		}
		isInFile := bloomFilter.Get([]byte(key))
		if compress2 {
			hashMap, err = GetHashMap(filePath, oneFile)
			if err != nil {
				panic(err)
			}
		}
		file.Close()

		if isInFile == true {
			offsetStart, offsetEnd, err3 := GetOffset(fileName, key, compress1, compress2, 0, 0, oneFile, 2, hashMap)
			if err3 == false {
				return data, false
			}
			offsetStart, offsetEnd, err3 = GetOffset(fileName, key, compress1, compress2, offsetStart, offsetEnd, oneFile, 3, hashMap)
			if err3 == false {
				return data, false
			}
			fmt.Printf("%d\n", offsetStart)
			data, err3 = ReadData(fileName, compress1, compress2, offsetStart, offsetEnd, key, oneFile, hashMap)
			if err3 == false {
				return data, false
			}

			return data, true
		}
	} else {
		file, err := os.OpenFile(filePath+"/BloomFilter.bin", os.O_RDONLY, 0666)
		if err != nil {
			return data, false
		}
		bloomFilter, err2 := bloomfilter.DeserializeBloomFilter(file)
		if err2 != nil {
			return data, false
		}
		isInFile := bloomFilter.Get([]byte(key))
		if isInFile == true {
			if compress2 {
				hashMap, err = GetHashMap(filePath, oneFile)
				if err != nil {
					panic(err)
				}
			}
			offsetStart, offsetEnd, err3 := GetOffset(filePath+"/Summary.bin", key, compress1, compress2, 0, 0, oneFile, 2, hashMap)
			if err3 == false {
				return data, false
			}
			offsetStart, offsetEnd, err3 = GetOffset(filePath+"/Index.bin", key, compress1, compress2, offsetStart, offsetEnd, oneFile, 3, hashMap)
			if err3 == false {
				return data, false
			}
			fmt.Printf("%d\n", offsetStart)
			data, err3 = ReadData(filePath+"/Data.bin", compress1, compress2, offsetStart, offsetEnd, key, oneFile, hashMap)
			if err3 == false {
				return data, false
			}

			return data, true
		}

	}

	return data, false
}

func ReadData(filePath string, compress1, compress2 bool, offsetStart, offsetEnd int64, key string, oneFile bool, hashMap map[string]int32) (datatype.DataType, bool) {
	Data := datatype.CreateDataType("", []byte(""))
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return *Data, false
	}
	defer file.Close()
	file.Seek(int64(offsetStart), 0)

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
		size, sizeEnd = positionInSSTable(*file, 5)
		if offsetEnd == 0 {
			offsetEnd = sizeEnd
		} else {
			offsetEnd += size
		}
		offsetStart += size
		end = sizeEnd - size
		if err != nil {
			return *Data, false
		}
	}
	if offsetEnd == 0 {
		offsetEnd = fileInfo.Size()
	}

	file.Seek(offsetStart, 0)
	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(offsetStart, 0)

	oldOffsetStart := offsetStart

	var currentKey string
	currentKey = ""
	var currentData []byte
	currentData = []byte("")
	data := datatype.CreateDataType(currentKey, currentData)
	for offsetStart <= offsetEnd {
		//read CRC
		bytes := make([]byte, 4)
		file.Seek(currentRead+oldOffsetStart, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		currentRead += 4

		// read timestamp
		bytes = make([]byte, 16)
		file.Seek(currentRead+oldOffsetStart, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		currentRead += 16
		//fmt.Printf("%d", bytes)
		nano := int64(binary.BigEndian.Uint64(bytes[8:]))
		timestamp = time.Unix(nano, 0)

		// read tombstone
		bytes = make([]byte, 1)
		file.Seek(currentRead+oldOffsetStart, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		tomb := int(bytes[0])
		currentRead += 1
		//fmt.Printf("%d", bytes)

		if compress2 {
			if compress1 {
				// read key size - ne postoji

				// read value size
				var valueSize int64
				var m int
				if tomb == 0 {
					valueSize, m = binary.Varint(bytesFile[currentRead:])
					currentRead += int64(m)
				}

				// read key
				tempKey, k := binary.Varint(bytesFile[currentRead:])
				currentKey = GetKeyByValue(&hashMap, int32(tempKey))
				//fmt.Printf("Key: %s ", ss)
				currentRead += int64(k)
				// read value
				if tomb == 0 {
					bytes = make([]byte, valueSize)
					file.Seek(currentRead+oldOffsetStart, 0)
					_, err = file.Read(bytes)
					if err != nil {
						panic(err)
					}
					//fmt.Printf("Value: %s", bytes)
					currentRead += int64(valueSize)
					currentData = bytes
				} else if currentKey == key {
					return *data, false
				}
				if currentKey == key {
					data.SetData(currentData)
					data.SetKey(currentKey)
					data.SetChangeTime(timestamp)
					return *data, true
				}
			} else {
				// read key size - znamo da je 4 bajta maks

				// read value size
				var valueSize uint64
				if tomb == 0 {

					buff := make([]byte, 8)
					file.Seek(currentRead+oldOffsetStart, 0)
					_, err = file.Read(buff)
					if err != nil {
						panic(err)
					}
					currentRead += 8
					valueSize = binary.BigEndian.Uint64(buff)
				}

				// read key
				buff := make([]byte, 4)
				_, err = file.Read(buff)
				if err != nil {
					panic(err)
				}
				currentRead += 4
				tempKey := binary.BigEndian.Uint32(buff)
				currentKey = GetKeyByValue(&hashMap, int32(tempKey))
				//fmt.Printf("Key : %s ", ss)

				// read value
				if tomb == 0 {
					buff = make([]byte, valueSize)
					_, err = file.Read(buff)
					if err != nil {
						panic(err)
					}
					//fmt.Printf("Value: %s", buff)
					currentRead += int64(valueSize)
					currentData = buff
				} else if currentKey == key {
					return *data, false
				}
				if currentKey == key {
					data.SetData(currentData)
					data.SetKey(currentKey)
					data.SetChangeTime(timestamp)
					return *data, true
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

				file.Seek(currentRead+oldOffsetStart, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				//fmt.Printf("Key: %s ", bytes)
				currentRead += keySize
				currentKey = string(bytes)
				// read value
				if tomb == 0 {
					bytes = make([]byte, valueSize)
					file.Seek(currentRead+oldOffsetStart, 0)
					_, err = file.Read(bytes)
					if err != nil {
						panic(err)
					}
					//fmt.Printf("Value: %s", bytes)
					currentRead += valueSize
					currentData = bytes
				} else if currentKey == key {
					return *data, false
				}
				if currentKey == key {
					data.SetData(currentData)
					data.SetKey(currentKey)
					data.SetChangeTime(timestamp)
					return *data, true
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
				currentKey = string(bytes)
				//fmt.Printf("Key: %s ", bytes)
				// read value
				if tomb == 0 {
					bytes = make([]byte, valueSize)
					_, err = file.Read(bytes)
					if err != nil {
						panic(err)
					}

					//fmt.Printf("Value: %s", bytes)
					currentRead += int64(valueSize)
					currentData = bytes
				} else if currentKey == key {
					return *data, false
				}
				if currentKey == key {
					data.SetData(currentData)
					data.SetKey(currentKey)
					data.SetChangeTime(timestamp)
					return *data, true
				}

			}
		}
		offsetStart = oldOffsetStart + currentRead
	}
	return *Data, false
}

func GetOffset(filePath, key string, compress1, compress2 bool, offsetStart, offsetEnd int64, oneFile bool, elem int, hashMap map[string]int32) (int64, int64, bool) {
	//ukoliko je u odvojenim fajlovima, prosljedjuje se cela putanja
	//ukoliko je u jednom prosledjuje se putanja da odgovarajuceg fajla SSTable
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

	if compress2 {
		elem += 1
	}

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

	file.Seek(offsetStart, 0)
	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(offsetStart, 0)
	var currentOffset int64
	currentOffset = 0
	oldOffsetStart := offsetStart
	for offsetStart <= offsetEnd {

		if compress2 {
			if compress1 {
				// ne treba key size jer radimo sa PutVarint
				// read key
				tempKey, k := binary.Varint(bytesFile[currentRead:])
				currentRead += int64(k)
				//fmt.Printf("Key: %d ", key)
				// read offset
				offset, m := binary.Varint(bytesFile[currentRead:])
				currentRead += int64(m)
				//fmt.Printf("Offset: %d \n", offset)
				currentKey := GetKeyByValue(&hashMap, int32(tempKey))
				if currentKey > key {
					return currentOffset, offset, true
				}
				//fmt.Printf("Offset: %d \n", binary.BigEndian.Uint32(bytes))
				if currentKey == key {
					//fmt.Printf("Offset: %d \n", currentOffset)
					currentOffset = offset
					return currentOffset, currentOffset, true
				}
			} else {
				// key size - makx 4 bajta
				// read key
				buff := make([]byte, 4)
				_, err = file.Read(buff)
				if err != nil {
					panic(err)
				}
				currentRead += 4
				tempKey := binary.BigEndian.Uint32(buff)
				//fmt.Printf("Kljuc : %d ", key)

				// read offset
				bytes := make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += 8
				//fmt.Printf("Offset: %d \n", binary.BigEndian.Uint64(bytes))
				currentKey := GetKeyByValue(&hashMap, int32(tempKey))
				if currentKey > key {
					offsetEnd = int64(binary.BigEndian.Uint64(bytes))
					return currentOffset, offsetEnd, true
				}
				//fmt.Printf("Offset: %d \n", binary.BigEndian.Uint32(bytes))
				if currentKey == key {
					currentOffset = int64(binary.BigEndian.Uint64(bytes))
					//fmt.Printf("Offset: %d \n", currentOffset)
					return currentOffset, currentOffset, true
				}
			}
		} else {
			if compress1 == true {
				// read key size
				keySize, n := binary.Varint(bytesFile[currentRead:])
				//fmt.Printf("procitano: %d", n)
				currentRead += int64(n)

				//Read keys
				bytes := make([]byte, keySize)
				file.Seek(currentRead+oldOffsetStart, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}

				currentRead += int64(keySize)
				//fmt.Printf("Kljuc : %s ", bytes)
				currentKey := string(bytes)
				//Read offset
				offset, m := binary.Varint(bytesFile[currentRead:])
				currentRead += int64(m)
				//fmt.Printf("Offset: %d \n", offset)
				if currentKey > key {
					return currentOffset, offset, true
				}
				//fmt.Printf("Offset: %d \n", binary.BigEndian.Uint32(bytes))
				if currentKey == key {
					currentOffset = offset
					//fmt.Printf("Offset: %d \n", currentOffset)
					return currentOffset, currentOffset, true
				}
			} else {
				// read key size
				bytes := make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += 8
				keySize := binary.BigEndian.Uint64(bytes)

				//Read key
				bytes = make([]byte, keySize)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += int64(keySize)
				//fmt.Printf("Kljuc : %s ", bytes)
				currentKey := string(bytes)
				//Read offset
				bytes = make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += 8
				//fmt.Printf("Offset: %d \n", binary.BigEndian.Uint64(bytes))
				if currentKey > key {
					offsetEnd = int64(binary.BigEndian.Uint64(bytes))
					return currentOffset, offsetEnd, true
				}
				//fmt.Printf("Offset: %d \n", binary.BigEndian.Uint32(bytes))
				if currentKey == key {
					currentOffset = int64(binary.BigEndian.Uint64(bytes))
					//fmt.Printf("Offset: %d \n", currentOffset)
					return currentOffset, currentOffset, true
				}
			}
		}
		offsetStart = oldOffsetStart + currentRead

	}
	return currentOffset, 0, true
}

func GetHashMap(filePath string, oneFile bool) (map[string]int32, error) {
	var decodeMap map[string]int32
	var start, end int64
	start = 0
	fileNameHash := filePath + "/HashMap.bin"
	if oneFile {
		fileNameHash = filePath + "/SSTable.bin"

	}

	fileHash, err := os.OpenFile(fileNameHash, os.O_RDONLY, 0666)
	if err != nil {
		return decodeMap, err
	}
	defer fileHash.Close()
	if oneFile {
		start, end = positionInSSTable(*fileHash, 1)
	} else {
		start = 0
		fileInfoHash, err := os.Stat(fileNameHash)
		if err != nil {
			panic(err)
		}

		end = fileInfoHash.Size()
	}

	fileHash.Seek(start, 0)
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
	return decodeMap, nil
}
