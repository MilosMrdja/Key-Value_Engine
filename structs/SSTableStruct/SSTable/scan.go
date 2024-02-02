package SSTable

import (
	"encoding/binary"
	"fmt"
	"os"
	"sstable/mem/memtable/datatype"
	"time"
)

func GetRecord(filePath string, beginOffset uint64, compress1, compress2 bool) (datatype.DataType, uint32) {
	oneFile := GetOneFile(filePath)
	fileName := filePath + "/Data.bin"
	if oneFile {
		fileName = filePath + "/SSTable.bin"
	}
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
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
	var KEY, VALUE string
	var TIME time.Time
	var DELETE bool

	var size, sizeEnd int64
	if oneFile {
		size, sizeEnd = PositionInSSTable(*file, 5)

		end = sizeEnd - size
		_, err1 := file.Seek(size+int64(beginOffset)+size, 0)
		if err1 != nil {
			panic(err)
		}
	} else {
		_, err = file.Seek(size+int64(beginOffset)+0, 0)
		if err != nil {
			panic(err)
		}
	}
	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(size+int64(beginOffset), 0)
	fmt.Printf("Velicina: %d\n", (end))

	// deserialization hashmap
	decodeMap, err := DeserializationHashMap("EncodedKeys.bin")
	if err != nil {
		panic(err)
	}

	file.Seek(size+int64(beginOffset), 0)

	//read CRC
	bytes := make([]byte, 4)
	file.Seek(size+int64(beginOffset)+currentRead+size, 0)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	currentRead += 4

	// read timestamp
	bytes = make([]byte, 16)
	file.Seek(size+int64(beginOffset)+currentRead+size, 0)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	currentRead += 16
	nano := int64(binary.BigEndian.Uint64(bytes[8:]))
	TIME = time.Unix(nano, 0)

	// read tombstone
	bytes = make([]byte, 1)
	file.Seek(size+int64(beginOffset)+currentRead+size, 0)
	_, err = file.Read(bytes)
	if err != nil {
		panic(err)
	}
	tomb := int(bytes[0])
	currentRead += 1
	if tomb == 0 {
		DELETE = false
	} else {
		DELETE = true
	}

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
			key, k := binary.Varint(bytesFile[currentRead:])
			KEY = GetKeyByValue(decodeMap, int32(key))
			//fmt.Printf("Key: %s ", ss)
			currentRead += int64(k)
			// read value
			if tomb == 0 {
				bytes = make([]byte, valueSize)
				file.Seek(size+int64(beginOffset)+currentRead+size, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				VALUE = string(bytes)
				//fmt.Printf("Value: %s", VALUE)
				currentRead += int64(valueSize)
			}
		} else {
			// read key size - znamo da je 4 bajta maks

			// read value size
			var valueSize uint64
			if tomb == 0 {

				buff := make([]byte, 8)
				file.Seek(size+int64(beginOffset)+currentRead+size, 0)
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
			key := binary.BigEndian.Uint32(buff)
			KEY = GetKeyByValue(decodeMap, int32(key))
			fmt.Printf("Key : %s ", KEY)

			// read value
			if tomb == 0 {
				buff = make([]byte, valueSize)
				_, err = file.Read(buff)
				if err != nil {
					panic(err)
				}
				VALUE = string(buff)
				//fmt.Printf("Value: %s", VALUE)
				currentRead += int64(valueSize)
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
			file.Seek(size+int64(beginOffset)+currentRead+size, 0)
			_, err = file.Read(bytes)
			if err != nil {
				panic(err)
			}
			KEY = string(bytes)
			fmt.Printf("Key: %s ", KEY)
			currentRead += keySize
			// read value
			if tomb == 0 {
				bytes = make([]byte, valueSize)
				file.Seek(size+int64(beginOffset)+currentRead+size, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				VALUE = string(bytes)
				fmt.Printf("Value: %s", VALUE)
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
			KEY = string(bytes)
			fmt.Printf("Key: %s ", KEY)
			// read value
			if tomb == 0 {
				bytes = make([]byte, valueSize)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				VALUE = string(bytes)
				fmt.Printf("Value: %s", VALUE)
				currentRead += int64(valueSize)
			}

		}

	}

	dataType := datatype.CreateDataType(KEY, []byte(VALUE), TIME)

	dataType.SetChangeTime(TIME)
	dataType.SetDelete(DELETE)
	return *dataType, uint32(currentRead)
}
