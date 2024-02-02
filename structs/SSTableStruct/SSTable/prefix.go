package SSTable

import (
	"encoding/binary"
	"os"
	"sstable/mem/memtable/datatype"
	"strings"
	"time"
)

func GetByPrefix(filePath string, prefix string, compress1, compress2 bool, number *int) ([]datatype.DataType, string, int64, bool) {
	oneFile := GetOneFile(filePath)

	var data []datatype.DataType
	var hashMap *map[string]int32
	var err error
	var fileName string
	if oneFile {
		fileName = filePath + "/SSTable.bin"
		if compress2 {
			hashMap, err = DeserializationHashMap("EncodedKeys.bin")
			if err != nil {
				panic(err)
			}
		}
	} else {
		fileName = filePath + "/Data.bin"
	}
	//procitaj summary
	//ako 1. sadrzi prefix citaj redom dok ne procitas sve ili dok ne nadjes prvi bez prefixa
	//ako 1. ne sadrzi, produzi dalje.....

	data, offset, err3 := ReadByPrefix(fileName, compress1, compress2, 0, 0, prefix, hashMap, number)
	if err3 == false {
		return data, "", 0, false
	}
	return data, fileName, offset, true
	return data, "", 0, false
}

func ReadByPrefix(filePath string, compress1, compress2 bool, offsetStart, offsetEnd int64, prefix string, hashMap *map[string]int32, number *int) ([]datatype.DataType, int64, bool) {
	oneFile := GetOneFile(filePath)

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	var result []datatype.DataType
	//Data := datatype.CreateDataType("", []byte(""))
	if err != nil {
		return result, 0, false
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
		if compress2 == true {
			size, sizeEnd = PositionInSSTable(*file, 4)
		} else {
			size, sizeEnd = PositionInSSTable(*file, 3)
		}
		if offsetEnd == 0 {
			offsetEnd = sizeEnd
		} else {
			offsetEnd += size
		}
		offsetStart += size
		end = sizeEnd - size
		if err != nil {
			return result, 0, false
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
	data := datatype.CreateDataType(currentKey, currentData, time.Now())
	for offsetStart <= offsetEnd {
		if *number == 0 {
			return result, offsetStart, true
		}
		//read CRC
		bytes := make([]byte, 4)
		file.Seek(currentRead+offsetStart, 0)
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
				}
				if strings.HasPrefix(currentKey, prefix) && tomb == 0 {
					data = datatype.CreateDataType(currentKey, currentData, timestamp)
					data.SetChangeTime(timestamp)
					result = append(result, *data)
					*number--
				}
				if !strings.HasPrefix(currentKey, prefix) && currentKey > prefix {
					return result, 0, true
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
				}
				if strings.HasPrefix(currentKey, prefix) && tomb == 0 {
					data = datatype.CreateDataType(currentKey, currentData, timestamp)
					data.SetChangeTime(timestamp)
					result = append(result, *data)
					*number--
				}
				if !strings.HasPrefix(currentKey, prefix) && currentKey > prefix {
					return result, 0, true
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
				}
				if strings.HasPrefix(currentKey, prefix) && tomb == 0 {
					data = datatype.CreateDataType(currentKey, currentData, timestamp)
					data.SetChangeTime(timestamp)
					result = append(result, *data)
					*number--
				}
				if !strings.HasPrefix(currentKey, prefix) && currentKey > prefix {
					return result, 0, true
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
				}
				if strings.HasPrefix(currentKey, prefix) && tomb == 0 {
					data = datatype.CreateDataType(currentKey, currentData, timestamp)
					data.SetChangeTime(timestamp)
					result = append(result, *data)
					*number--
				}
				if !strings.HasPrefix(currentKey, prefix) && currentKey > prefix {
					return result, 0, true
				}
			}
		}

		offsetStart = oldOffsetStart + currentRead
	}
	return result, offsetStart, true
}
