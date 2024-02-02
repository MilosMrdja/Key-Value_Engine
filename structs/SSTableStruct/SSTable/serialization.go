package SSTable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"log"
	"os"
	"sstable/mem/memtable/datatype"
)

type Tuple struct {
	X string
	Y int32
}

// funkcija za upis podatka u Index
func SerializeIndexData(key string, length int, compress1, compress2 bool, keyDict int32) ([]byte, error) {
	var result bytes.Buffer
	// write key size
	// if an user wants to compres file

	// write keysize
	//var nCompres2 int
	if !compress2 {
		if compress1 {
			buff := make([]byte, 8)
			n := binary.PutVarint(buff, int64(len(key)))
			result.Write(buff[:n])
		} else {
			err := binary.Write(&result, binary.BigEndian, uint64(len(key)))
			if err != nil {
				return nil, err
			}
		}
	}
	// write key
	if compress2 {
		if compress1 {
			buff := make([]byte, 4) // niz velicine keySize
			n := binary.PutVarint(buff, int64(keyDict))
			result.Write(buff[:n])
		} else {
			err := binary.Write(&result, binary.BigEndian, uint32(keyDict))
			if err != nil {
				panic(err)
			}

		}

	} else {
		result.Write([]byte(key))
	}
	//Write length
	if compress1 {
		buf := make([]byte, 8)
		n := binary.PutVarint(buf, int64(length))
		//fmt.Printf(" 2.  %d", n)
		result.Write(buf[:n])
	} else {
		err := binary.Write(&result, binary.BigEndian, uint64(length))
		if err != nil {
			return []byte(""), err
		}
	}

	return result.Bytes(), nil
}

// key size, value size, timestamp - kompresija
// f-ja koja serijalizuje jedan podatak iz memtabele

func SerializeDataType(data datatype.DataType, compress1, compress2 bool, keyDict int32) ([]byte, error) {
	var result, tempRes bytes.Buffer

	//create and write timestamp
	TimeBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(TimeBytes[8:], uint64(data.GetChangeTime().Unix()))
	tempRes.Write(TimeBytes)

	// Write tombstone
	tomb := byte(0)
	if data.GetDelete() == true {
		tomb = 1
	}
	tempRes.WriteByte(tomb)

	currentData := data.GetData()
	currentKey := data.GetKey()
	//var nCompres2 int
	// write key size

	if !compress2 {
		if compress1 {
			buff := make([]byte, 8)
			n := binary.PutVarint(buff, int64(len(currentKey)))
			tempRes.Write(buff[:n])
		} else {
			err := binary.Write(&tempRes, binary.BigEndian, uint64(len(currentKey)))
			if err != nil {
				return nil, err
			}
		}
	}

	if tomb == 0 {
		// write value size
		if compress1 {
			buff := make([]byte, 8)
			n := binary.PutVarint(buff, int64(len(currentData)))
			tempRes.Write(buff[:n])
		} else {
			err := binary.Write(&tempRes, binary.BigEndian, uint64(len(currentData)))
			if err != nil {
				return nil, err
			}
		}
	}

	// write key
	if compress2 {
		if compress1 {
			buff := make([]byte, 4)
			n := binary.PutVarint(buff, int64(keyDict))
			tempRes.Write(buff[:n])

		} else {
			err := binary.Write(&tempRes, binary.BigEndian, uint32(keyDict))
			if err != nil {
				panic(err)
			}
		}
	} else {
		// u slucaju i sa i bez prve kompresije radi ovo
		tempRes.Write([]byte(currentKey))
	}

	if tomb == 0 {
		// write value
		tempRes.Write(currentData)
	}

	//create and write CRC
	crc := crc32.ChecksumIEEE(tempRes.Bytes())
	err := binary.Write(&result, binary.BigEndian, crc)
	if err != nil {
		return nil, nil
	}
	result.Write(tempRes.Bytes())

	return result.Bytes(), nil
}

// Dodati u conf file sledece konstante pri citanju i njihove sizeof
// 0 - Bloomfilter
// 1 - hashmapa ako je potrebno
// 2 - summary deo
// 3 - index deo
// 4 - data deo
// 5 - Merkle tree
func WriteToOneFile(bloom, summary, index, data, merkle string) ([]byte, error) {
	var result bytes.Buffer
	var tempArr []byte

	var segmentLength []int32
	var tempLength int32

	fileInfo, err := os.Stat(data)
	if err != nil {
		panic(err)
	}
	end := fileInfo.Size()
	segmentLength = append(segmentLength, int32(end))

	tempArr, tempLength = getFileInfo(merkle)
	result.Write(tempArr)
	segmentLength = append(segmentLength, tempLength+segmentLength[0])

	tempArr, tempLength = getFileInfo(index)
	result.Write(tempArr)
	segmentLength = append(segmentLength, tempLength+segmentLength[1])

	tempArr, tempLength = getFileInfo(summary)
	result.Write(tempArr)
	segmentLength = append(segmentLength, tempLength+segmentLength[2])

	tempArr, tempLength = getFileInfo(bloom)
	result.Write(tempArr)
	segmentLength = append(segmentLength, tempLength+segmentLength[3])

	for i := 0; i < len(segmentLength); i++ {
		err = binary.Write(&result, binary.BigEndian, segmentLength[i])
		if err != nil {
			result.Reset()
			return result.Bytes(), nil
		}
	}
	return result.Bytes(), nil
}

func getFileInfo(fileName string) ([]byte, int32) {

	var result bytes.Buffer
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fileInfo, err := os.Stat(fileName)
	if err != nil {
		panic(err)
	}

	end := fileInfo.Size()

	byteArr := make([]byte, end)
	_, err = file.Read(byteArr)
	if err != nil {
		panic(err)
	}

	result.Write(byteArr)

	return result.Bytes(), int32(end)
}

func SerializeHashmap(filename string, mapa *map[string]int32) ([]byte, error) {

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	if err := os.Truncate(filename, 0); err != nil {
		log.Printf("Failed to truncate: %v", err)
	}

	_, err = os.Stat(filename)
	if err != nil {
		return nil, err
	}
	var result bytes.Buffer
	var buff1 []byte
	for k, v := range *mapa {
		// write key size
		buff1 = make([]byte, 4)
		binary.BigEndian.PutUint32(buff1, uint32(len(k)))
		result.Write(buff1)

		//write key
		result.Write([]byte(k))

		// write hashed key
		buff1 = make([]byte, 4)
		binary.BigEndian.PutUint32(buff1, uint32(v))
		result.Write(buff1)
	}

	_, err = file.Write(result.Bytes())
	if err != nil {
		return nil, err
	}
	return result.Bytes(), nil

}
func SerializeTuples(filename string, tuples []Tuple) ([]byte, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var buf bytes.Buffer
	for _, tuple := range tuples {
		// Write X length
		xLen := uint32(len(tuple.X))
		if err := binary.Write(&buf, binary.BigEndian, xLen); err != nil {
			return nil, err
		}

		// Write X
		if _, err := buf.WriteString(tuple.X); err != nil {
			return nil, err
		}

		// Write Y
		if err := binary.Write(&buf, binary.BigEndian, tuple.Y); err != nil {
			return nil, err
		}
	}

	_, err = file.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
