package SSTable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"sstable/mem/memtable/datatype"
	"unsafe"
)

// funkcija za upis podatka u Index
func SerializeIndexData(key string, length int, compress1, compress2 bool, keyDict int32) ([]byte, error) {
	var result bytes.Buffer
	// write key size
	// if an user wants to compres file

	// write keysize
	//var nCompres2 int
	if compress2 {
		if compress1 {
			//buff := make([]byte, 4)
			//nCompres2 = binary.PutVarint(buff, int64(keyDict))
			//buff1 := make([]byte, 1)
			//buff1[0] = byte(nCompres2)
			//result.Write(buff1)
		} else {
			err := binary.Write(&result, binary.BigEndian, int8(4)) // jer je maks 4 bajta
			if err != nil {
				return nil, err
			}
		}
	} else {
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
		err := binary.Write(&result, binary.BigEndian, int64(length))
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
	var nCompres2 int
	// write key size

	if compress2 {
		if compress1 {
			buff := make([]byte, 4)
			nCompres2 = binary.PutVarint(buff, int64(keyDict))
			buff1 := make([]byte, 1)
			buff1[0] = byte(nCompres2)
			result.Write(buff1)
		} else {
			err = binary.Write(&result, binary.BigEndian, uint64(4)) // jer je maks 4 bajta
			if err != nil {
				return nil, err
			}
		}
	} else {
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
			err = binary.Write(&result, binary.BigEndian, uint64(keyDict))
			if err != nil {
				return nil, err
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

func IntToByteArray(num int32) []byte {
	size := int(unsafe.Sizeof(num))
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		byt := *(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&num)) + uintptr(i)))
		arr[i] = byt
	}
	return arr
}

// Dodati u conf file sledece konstante pri citanju i njihove sizeof
// 0 - Bloomfilter
// 1 - summary deo
// 2 - index deo
// 3 - data deo
// 4 - Merkle tree
func WriteToOneFile(bloom, summary, index, data, merkle string) ([]byte, error) {
	var result bytes.Buffer
	var tempArr []byte

	tempArr = getFileInfo(bloom, 0)
	result.Write(tempArr)

	tempArr = getFileInfo(summary, 1)
	result.Write(tempArr)

	tempArr = getFileInfo(index, 2)
	result.Write(tempArr)

	tempArr = getFileInfo(data, 3)
	result.Write(tempArr)

	tempArr = getFileInfo(merkle, 4)
	result.Write(tempArr)

	return result.Bytes(), nil
}

func getFileInfo(fileName string, n int) []byte {

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

	//id := make([]byte, 1)
	//id[0] = byte(n)
	//result.Write(id)

	end := fileInfo.Size()

	//upisuje duzinu dela
	err = binary.Write(&result, binary.BigEndian, uint64(end))
	if err != nil {
		result.Reset()
		return result.Bytes()
	}

	byteArr := make([]byte, end)
	_, err = file.Read(byteArr)
	if err != nil {
		panic(err)
	}

	result.Write(byteArr)

	return result.Bytes()
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
