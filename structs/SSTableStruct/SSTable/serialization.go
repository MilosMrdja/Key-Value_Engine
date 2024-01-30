package SSTable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"sstable/mem/memtable/datatype"
)

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
	var result bytes.Buffer

	//create and write CRC
	crc := crc32.ChecksumIEEE(data.GetData())
	err := binary.Write(&result, binary.BigEndian, crc)
	if err != nil {
		return nil, nil
	}

	//create and write timestamp
	TimeBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(TimeBytes[8:], uint64(data.GetChangeTime().Unix()))
	result.Write(TimeBytes)

	// Write tombstone
	tomb := byte(0)
	if data.GetDelete() == true {
		tomb = 1
	}
	result.WriteByte(tomb)

	currentData := data.GetData()
	currentKey := data.GetKey()
	//var nCompres2 int
	// write key size

	if !compress2 {
		if compress1 {
			buff := make([]byte, 8)
			n := binary.PutVarint(buff, int64(len(currentKey)))
			result.Write(buff[:n])
		} else {
			err = binary.Write(&result, binary.BigEndian, uint64(len(currentKey)))
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
			result.Write(buff[:n])
		} else {
			err = binary.Write(&result, binary.BigEndian, uint64(len(currentData)))
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
			err = binary.Write(&result, binary.BigEndian, uint32(keyDict))
			if err != nil {
				panic(err)
			}
		}
	} else {
		// u slucaju i sa i bez prve kompresije radi ovo
		result.Write([]byte(currentKey))
	}

	if tomb == 0 {
		// write value
		result.Write(currentData)
	}

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

func SerializeHashmap(filename string, bytes []byte) error {
	_, err := os.Stat(filename)
	if err == nil {
		err1 := os.Remove(filename)
		if err1 != nil {
			return err1
		}
	}

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
	}

	defer file.Close()

	_, err = file.Write(bytes)
	if err != nil {
		return err
	}
	return nil

}
