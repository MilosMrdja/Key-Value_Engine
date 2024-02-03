package SSTable

import (
	"encoding/binary"
	"os"
	"sstable/mem/memtable/datatype"
)

// prva vrednost je min,druga je max
func GetSummaryMinMax(filePath string, compress1, compress2 bool) (datatype.DataType, datatype.DataType, int64) {
	oneFile := GetOneFile(filePath)

	var minData, maxData datatype.DataType
	fileName := "/Summary.bin"
	if oneFile {
		fileName = "/SSTable.bin"
	}
	filePath += fileName
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		return minData, maxData, 0
	}
	defer file.Close()

	fileInfo, err := os.Stat(filePath)
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
		size, sizeEnd = PositionInSSTable(*file, 2)
		end = sizeEnd - size
		_, err1 := file.Seek(size, 0)
		if err1 != nil {
			return minData, maxData, 0
		}
	} else {
		_, err = file.Seek(0, 0)
		if err != nil {
			return minData, maxData, 0
		}
	}
	bytesFile := make([]byte, end)
	_, err = file.Read(bytesFile)
	if err != nil {
		panic(err)
	}
	file.Seek(size, 0)
	//var keySize int

	minData.SetKey("")
	maxData.SetKey("")

	for currentRead != end {

		if compress2 {
			if compress1 {
				// ne treba key size jer radimo sa PutVarint
				// read key
				key, k := binary.Varint(bytesFile[currentRead:])
				currentRead += int64(k)
				ss := GetKeyByValue(decodeMap, int32(key))
				// read offset
				_, m := binary.Varint(bytesFile[currentRead:])
				currentRead += int64(m)
				if minData.GetKey() == "" {
					minData.SetKey(ss)
				} else if maxData.GetKey() == "" {
					maxData.SetKey(ss)
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
				key := binary.BigEndian.Uint32(buff)
				ss := GetKeyByValue(decodeMap, int32(key))

				// read offset
				bytes := make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += 8
				if minData.GetKey() == "" {
					minData.SetKey(ss)
				} else if maxData.GetKey() == "" {
					maxData.SetKey(ss)
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
				file.Seek(currentRead+size, 0)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}

				currentRead += int64(keySize)
				ss := string(bytes)
				//Read offset
				_, m := binary.Varint(bytesFile[currentRead:])
				currentRead += int64(m)
				if minData.GetKey() == "" {
					minData.SetKey(ss)
				} else if maxData.GetKey() == "" {
					maxData.SetKey(ss)
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
				ss := string(bytes)
				//Read offset
				bytes = make([]byte, 8)
				_, err = file.Read(bytes)
				if err != nil {
					panic(err)
				}
				currentRead += 8
				if minData.GetKey() == "" {
					minData.SetKey(ss)
				} else if maxData.GetKey() == "" {
					maxData.SetKey(ss)
				}
			}
		}
		if minData.GetKey() != "" && maxData.GetKey() != "" {
			break
		}
	}
	return minData, maxData, currentRead
}
