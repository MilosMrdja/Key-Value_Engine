package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sstable/bloomfilter/bloomfilter"
	"sstable/mem/memtable/datatype"
	"time"
)

func GetData(filePath string, key string, compress1, compress2 bool) (datatype.DataType, bool) {
	oneFile := GetOneFile(filePath)

	var data datatype.DataType
	var hashMap *map[string]int32
	if oneFile {
		fileName := filePath + "/SSTable.bin"
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
		if err != nil {
			return data, false
		}
		size, _ := PositionInSSTable(*file, 1)
		file.Seek(size, 0)
		bloomFilter, err2 := bloomfilter.ReadFromFile(file)
		if err2 != nil {
			return data, false
		}
		isInFile := bloomFilter.Get([]byte(key))
		if compress2 {
			hashMap, err = DeserializationHashMap("EncodedKeys.bin")
			if err != nil {
				panic(err)
			}
		}
		file.Close()

		if isInFile == true {
			offsetStart, offsetEnd, err3 := GetOffset(filePath, key, compress1, compress2, 0, 0, 2, hashMap)
			if err3 == false {
				return data, false
			}
			offsetStart, offsetEnd, err3 = GetOffset(filePath, key, compress1, compress2, offsetStart, offsetEnd, 3, hashMap)
			if err3 == false {
				return data, false
			}
			fmt.Printf("%d\n", offsetStart)
			data, err3 = ReadData(filePath, compress1, compress2, offsetStart, offsetEnd, key, hashMap)
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
		bloomFilter, err2 := bloomfilter.ReadFromFile(file)
		if err2 != nil {
			return data, false
		}
		isInFile := bloomFilter.Get([]byte(key))
		if isInFile == true {
			if compress2 {
				hashMap, err = DeserializationHashMap("EncodedKeys.bin")
				if err != nil {
					panic(err)
				}
				if err != nil {
					panic(err)
				}
			}
			offsetStart, offsetEnd, err3 := GetOffset(filePath, key, compress1, compress2, 0, 0, 2, hashMap)
			if err3 == false {
				return data, false
			}
			offsetStart, offsetEnd, err3 = GetOffset(filePath, key, compress1, compress2, offsetStart, offsetEnd, 3, hashMap)
			if err3 == false {
				return data, false
			}
			data, err3 = ReadData(filePath, compress1, compress2, offsetStart, offsetEnd, key, hashMap)
			if err3 == false {
				return data, false
			}

			return data, true
		}

	}

	return data, false
}

func ReadData(filePath string, compress1, compress2 bool, offsetStart, offsetEnd int64, key string, hashMap *map[string]int32) (datatype.DataType, bool) {
	oneFile := GetOneFile(filePath)
	var crc, tempCRC bytes.Buffer
	if !oneFile {
		filePath += "/Data.bin"
	} else {
		filePath += "/SSTable.bin"
	}
	Data := datatype.CreateDataType("", []byte(""), time.Now())
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
		size, sizeEnd = PositionInSSTable(*file, 5)
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
	data := datatype.CreateDataType(currentKey, currentData, timestamp)
	for offsetStart <= offsetEnd {
		crc.Reset()
		tempCRC.Reset()
		//read CRC
		bytes := make([]byte, 4)
		file.Seek(currentRead+oldOffsetStart, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		currentRead += 4
		crc.Write(bytes)

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
		tempCRC.Write(bytes)

		// read tombstone
		bytes = make([]byte, 1)
		file.Seek(currentRead+oldOffsetStart, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		tomb := int(bytes[0])
		currentRead += 1
		tempCRC.Write(bytes)
		//fmt.Printf("%d", bytes)

		if compress2 {
			if compress1 {
				// read key size - ne postoji
				// read value size
				var valueSize int64
				var m int
				if tomb == 0 {
					valueSize, m = binary.Varint(bytesFile[currentRead:])

					next := currentRead + int64(m)
					tempCRC.Write(bytesFile[currentRead:next])
					currentRead += int64(m)
				}

				// read key
				tempKey, k := binary.Varint(bytesFile[currentRead:])
				next := currentRead + int64(k)
				tempCRC.Write(bytesFile[currentRead:next])
				currentKey = GetKeyByValue(hashMap, int32(tempKey))
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
					tempCRC.Write(bytes)
				} else if currentKey == key {
					return *data, false
				}
				if currentKey == key {
					data.SetData(currentData)
					data.SetKey(currentKey)
					data.SetChangeTime(timestamp)
					if crc32.ChecksumIEEE(tempCRC.Bytes()) != binary.BigEndian.Uint32(crc.Bytes()) {
						data.SetKey("")
					}
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
					tempCRC.Write(buff)
				}

				// read key
				buff := make([]byte, 4)
				_, err = file.Read(buff)
				if err != nil {
					panic(err)
				}
				currentRead += 4
				tempKey := binary.BigEndian.Uint32(buff)
				tempCRC.Write(buff)
				currentKey = GetKeyByValue(hashMap, int32(tempKey))
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
					tempCRC.Write(buff)
				} else if currentKey == key {
					return *data, false
				}
				if currentKey == key {
					data.SetData(currentData)
					data.SetKey(currentKey)
					data.SetChangeTime(timestamp)
					if crc32.ChecksumIEEE(tempCRC.Bytes()) != binary.BigEndian.Uint32(crc.Bytes()) {
						data.SetKey("")
					}
					return *data, true
				}

			}
		} else {
			if compress1 {
				// read key size
				keySize, n := binary.Varint(bytesFile[currentRead:])
				next := currentRead + int64(n)
				tempCRC.Write(bytesFile[currentRead:next])
				currentRead += int64(n)
				// read value size
				var valueSize int64
				var m int
				if tomb == 0 {
					valueSize, m = binary.Varint(bytesFile[currentRead:])

					next = currentRead + int64(n)
					tempCRC.Write(bytesFile[currentRead:next])
					currentRead += int64(m)
				}
				// read key
				bytes = make([]byte, keySize)

				file.Seek(currentRead+oldOffsetStart, 0)
				_, err = file.Read(bytes)
				tempCRC.Write(bytes)
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
					tempCRC.Write(bytes)
				} else if currentKey == key {
					return *data, false
				}
				if currentKey == key {
					data.SetData(currentData)
					data.SetKey(currentKey)
					data.SetChangeTime(timestamp)
					t := crc32.ChecksumIEEE(tempCRC.Bytes())
					t1 := binary.BigEndian.Uint32(crc.Bytes())
					if t != t1 {
						data.SetKey("")
					}
					return *data, true
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
				//fmt.Printf("%d", bytes)
				var valueSize uint64
				if tomb == 0 {
					// read value size
					bytes = make([]byte, 8)
					_, err = file.Read(bytes)
					if err != nil {
						panic(err)
					}
					tempCRC.Write(bytes)
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
				tempCRC.Write(bytes)
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
					tempCRC.Write(bytes)
				} else if currentKey == key {
					return *data, false
				}
				if currentKey == key {
					data.SetData(currentData)
					data.SetKey(currentKey)
					data.SetChangeTime(timestamp)
					if crc32.ChecksumIEEE(tempCRC.Bytes()) != binary.BigEndian.Uint32(crc.Bytes()) {
						data.SetKey("")
					}
					return *data, true
				}

			}
		}
		offsetStart = oldOffsetStart + currentRead
		if offsetStart >= offsetEnd {
			break
		}
	}
	return *Data, false
}

func GetOffset(filePath, key string, compress1, compress2 bool, offsetStart, offsetEnd int64, elem int, hashMap *map[string]int32) (int64, int64, bool) {
	//ukoliko je u odvojenim fajlovima, prosljedjuje se cela putanja
	//ukoliko je u jednom prosledjuje se putanja da odgovarajuceg fajla SSTable

	oneFile := GetOneFile(filePath)
	var summaryRead int64
	summaryRead = 0
	if elem == 2 {
		_, _, summaryRead = GetSummaryMinMax(filePath, compress1, compress2)
	}
	if !oneFile {
		filePath += "/Summary.bin"
	} else {
		filePath += "/SSTable.bin"
	}
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return 0, 0, false
	}
	defer file.Close()

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		panic(err)
	}
	var size, sizeEnd, end int64
	sizeEnd = fileInfo.Size()

	if oneFile {
		size, sizeEnd = PositionInSSTable(*file, elem)

		if offsetEnd == 0 {
			offsetEnd = sizeEnd
		} else {
			offsetEnd += size
		}
		offsetStart += size

		end = sizeEnd - size - summaryRead
		if err != nil {
			return 0, 0, false
		}
	}
	if offsetEnd == 0 {
		offsetEnd = fileInfo.Size()
	}
	var currentRead int64
	currentRead = 0

	offsetStart += summaryRead
	file.Seek(offsetStart, 0)
	end = sizeEnd - size - summaryRead
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
		if offsetStart == sizeEnd {
			break
		}

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
				currentKey := GetKeyByValue(hashMap, int32(tempKey))
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
				currentKey := GetKeyByValue(hashMap, int32(tempKey))
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
		if offsetStart >= offsetEnd {
			break
		}

	}
	return currentOffset, 0, true
}
