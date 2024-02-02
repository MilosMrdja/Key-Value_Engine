package SSTable

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
)

// vraca uspesnot i niz za merkle
func ReadSSTable(filePath string, compress1, compress2 bool) (bool, [][]byte) {

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

	var size, sizeEnd int64
	if oneFile {
		size, sizeEnd = PositionInSSTable(*file, 5)

		end = sizeEnd - size
		_, err1 := file.Seek(size, 0)
		if err1 != nil {
			panic(err)
		}
	} else {
		_, err = file.Seek(0, 0)
		if err != nil {
			panic(err)
		}
	}
	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(size, 0)
	fmt.Printf("Velicina: %d\n", (end))

	// deserialization hashmap
	decodeMap, err := DeserializationHashMap("EncodedKeys.bin")
	if err != nil {
		panic(err)
	}

	file.Seek(size, 0)
	var merkleArr [][]byte
	var merkleTreeTemp []byte
	var nextTree int64

	for currentRead != end {
		merkleTreeTemp = make([]byte, 0)
		//read CRC
		bytes := make([]byte, 4)
		file.Seek(currentRead+size, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		merkleTreeTemp = append(merkleTreeTemp, bytes...)
		currentRead += 4

		// read timestamp
		bytes = make([]byte, 16)
		file.Seek(currentRead+size, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		merkleTreeTemp = append(merkleTreeTemp, bytes...)

		currentRead += 16
		//fmt.Printf("%d", bytes)

		// read tombstone
		bytes = make([]byte, 1)
		file.Seek(currentRead+size, 0)
		_, err = file.Read(bytes)
		if err != nil {
			panic(err)
		}
		tomb := int(bytes[0])
		merkleTreeTemp = append(merkleTreeTemp, bytes...)
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
					nextTree = currentRead + int64(m)
					merkleTreeTemp = append(merkleTreeTemp, bytesFile[currentRead:nextTree]...)
					currentRead += int64(m)
				}

				// read key
				key, k := binary.Varint(bytesFile[currentRead:])
				nextTree = currentRead + int64(k)
				merkleTreeTemp = append(merkleTreeTemp, bytesFile[currentRead:nextTree]...)
				ss := GetKeyByValue(decodeMap, int32(key))
				fmt.Printf("Key: %s ", ss)
				currentRead += int64(k)
				// read value
				if tomb == 0 {
					bytes = make([]byte, valueSize)
					file.Seek(currentRead+size, 0)
					_, err = file.Read(bytes)
					if err != nil {
						panic(err)
					}
					fmt.Printf("Value: %s", bytes)
					merkleTreeTemp = append(merkleTreeTemp, bytes...)
					currentRead += int64(valueSize)
				}
			} else {
				// read key size - znamo da je 4 bajta maks

				// read value size
				var valueSize uint64
				if tomb == 0 {

					buff := make([]byte, 8)
					file.Seek(currentRead+size, 0)
					_, err = file.Read(buff)
					if err != nil {
						panic(err)
					}
					merkleTreeTemp = append(merkleTreeTemp, buff...)
					currentRead += 8
					valueSize = binary.BigEndian.Uint64(buff)
				}

				// read key
				buff := make([]byte, 4)
				_, err = file.Read(buff)
				if err != nil {
					panic(err)
				}
				merkleTreeTemp = append(merkleTreeTemp, buff...)
				currentRead += 4
				key := binary.BigEndian.Uint32(buff)
				ss := GetKeyByValue(decodeMap, int32(key))
				fmt.Printf("Key : %s ", ss)

				// read value
				if tomb == 0 {
					buff = make([]byte, valueSize)
					_, err = file.Read(buff)
					if err != nil {
						panic(err)
					}
					fmt.Printf("Value: %s", buff)
					merkleTreeTemp = append(merkleTreeTemp, buff...)
					currentRead += int64(valueSize)
				}

			}
		} else {
			if compress1 {
				// read key size
				keySize, n := binary.Varint(bytesFile[currentRead:])
				nextTree = currentRead + int64(n)
				merkleTreeTemp = append(merkleTreeTemp, bytesFile[currentRead:nextTree]...)
				currentRead += int64(n)
				// read value size
				var valueSize int64
				var m int
				if tomb == 0 {
					valueSize, m = binary.Varint(bytesFile[currentRead:])
					nextTree = currentRead + int64(m)
					merkleTreeTemp = append(merkleTreeTemp, bytesFile[currentRead:nextTree]...)
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
				merkleTreeTemp = append(merkleTreeTemp, bytes...)
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
					merkleTreeTemp = append(merkleTreeTemp, bytes...)
					currentRead += valueSize
				}

			} else {
				//read key size
				bytes = make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				merkleTreeTemp = append(merkleTreeTemp, bytes...)
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
					merkleTreeTemp = append(merkleTreeTemp, bytes...)
					currentRead += 8
					//fmt.Printf("%d", bytes)
				} // read key
				bytes = make([]byte, keySize)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				merkleTreeTemp = append(merkleTreeTemp, bytes...)
				currentRead += int64(keySize)
				fmt.Printf("Key: %s ", bytes)
				// read value
				if tomb == 0 {
					bytes = make([]byte, valueSize)
					_, err = file.Read(bytes)
					if err != nil {
						panic(err)
					}
					merkleTreeTemp = append(merkleTreeTemp, bytes...)
					fmt.Printf("Value: %s", bytes)
					currentRead += int64(valueSize)
				}

			}

		}
		fmt.Printf("\n")
		merkleArr = append(merkleArr, merkleTreeTemp)
	}

	return true, merkleArr
}

func GetKeyByValue(mapa *map[string]int32, val int32) string {
	for k, v := range *mapa {
		if v == val {
			return k
		}
	}
	return ""
}

func ReadIndex(fileName string, compress1, compress2 bool, elem int) bool {

	oneFile := GetOneFile(fileName)

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
	decodeMap, err := DeserializationHashMap("EncodedKeys.bin")
	if err != nil {
		panic(err)
	}
	var size, sizeEnd int64
	if oneFile {
		size, sizeEnd = PositionInSSTable(*file, elem)
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
	//var keySize int
	for currentRead != end {

		if compress2 {
			if compress1 {
				// ne treba key size jer radimo sa PutVarint
				// read key
				key, k := binary.Varint(bytesFile[currentRead:])
				currentRead += int64(k)
				ss := GetKeyByValue(decodeMap, int32(key))
				fmt.Printf("Key: %s ", ss)
				// read offset
				offset, m := binary.Varint(bytesFile[currentRead:])
				currentRead += int64(m)
				fmt.Printf("Offset: %d \n", offset)
			} else {
				// key size - makx 4 bajta
				// read key
				buff := make([]byte, 4)
				_, err = file.Read(buff)
				if err != nil {
					panic(err)
				}
				currentRead += 4
				key := binary.BigEndian.Uint32(buff)
				ss := GetKeyByValue(decodeMap, int32(key))
				fmt.Printf("Kljuc : %s ", ss)

				// read offset
				bytes := make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += 8
				fmt.Printf("Offset: %d \n", binary.BigEndian.Uint64(bytes))

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

func DeserializationHashMap(fileName string) (*map[string]int32, error) {

	file, err := os.OpenFile(fileName, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}

	fInfo, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, err
	}
	end := fInfo.Size()
	current := int64(0)
	mapa := make(map[string]int32)
	var buff []byte
	var keySize uint32
	var key string
	var value uint32
	for current != end {
		// read key size
		buff = make([]byte, 4)
		err := binary.Read(file, binary.BigEndian, buff)
		if err != nil {
			return nil, err
		}
		keySize = binary.BigEndian.Uint32(buff)
		current += 4

		// read key
		buff = make([]byte, keySize)
		_, err = file.Read(buff)
		if err != nil {
			return nil, err
		}
		key = string(buff)
		current += int64(keySize)

		// read value
		buff = make([]byte, 4)
		err = binary.Read(file, binary.BigEndian, buff)
		if err != nil {
			return nil, err
		}
		value = binary.BigEndian.Uint32(buff)
		current += 4

		// create map el
		mapa[key] = int32(value)

	}

	return &mapa, nil
}

func DeserializeTuples(filename string) ([]Tuple, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var tuples []Tuple

	for {
		var xLen uint32
		if err := binary.Read(file, binary.BigEndian, &xLen); err != nil {
			break // Assuming EOF reached
		}

		xBytes := make([]byte, xLen)
		if _, err := file.Read(xBytes); err != nil {
			return nil, err
		}

		var y int32
		if err := binary.Read(file, binary.BigEndian, &y); err != nil {
			return nil, err
		}

		tuples = append(tuples, Tuple{X: string(xBytes), Y: y})
	}

	return tuples, nil
}

// prosledjuje se npr. (sstable1)
func GetOneFile(filePath string) bool {
	folders, err := ioutil.ReadDir(filePath)
	if err != nil {
		panic(err)
	}
	if len(folders) > 1 {
		return false
	} else {
		return true
	}
}

// file name = (sstable1)
func DeserializeMerkleFromSST(fileName string) []byte {

	oneFile := GetOneFile(fileName)
	var merkleTreeByte []byte

	if oneFile {
		//TODO
		return nil
	} else {
		fileMerkle, err := os.OpenFile(fileName+"/Merkle.bin", os.O_RDONLY, 0666)
		if err != nil {
			panic(err)
		}
		info, err := os.Stat(fileName + "/Merkle.bin")
		if err != nil {
			panic(err)
		}
		merkleTreeByte = make([]byte, info.Size())
		_, err = fileMerkle.Read(merkleTreeByte)
		if err != nil {
			panic(err)
		}
		return merkleTreeByte
	}
}
