package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sstable/mem/memtable/datatype"
)

// funkcija za upis podatka u Index
func SerializeIndexData(key string, length int, compres bool) ([]byte, error) {
	var result bytes.Buffer
	// write key size
	// if an user wants to compres file
	if compres {
		buf := make([]byte, 3)
		n := binary.PutVarint(buf, int64(len(key)))
		fmt.Printf(" 1.  %d", n)
		result.Write(buf[:n])
	} else {

		err := binary.Write(&result, binary.BigEndian, uint32(len(key)))
		if err != nil {
			return []byte(""), err
		}
	}
	// write key
	result.Write([]byte(key))
	//Write length
	if compres {
		buf1 := make([]byte, 4)
		n := binary.PutVarint(buf1, int64(length))
		fmt.Printf(" 2.  %d", n)
		result.Write(buf1[:n])
	} else {
		err := binary.Write(&result, binary.BigEndian, uint32(length))
		if err != nil {
			return []byte(""), err
		}
	}

	return result.Bytes(), nil
}

// key size, value size, tspemt
// f-ja koja serijalizuje jedan podatak iz memtabele
func SerializeDataType(data datatype.DataType) ([]byte, error) {
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
	// write key size
	err = binary.Write(&result, binary.BigEndian, uint64(len(currentKey)))
	if err != nil {
		return nil, err
	}

	if tomb == 0 {
		// write value size
		err = binary.Write(&result, binary.BigEndian, uint64(len(currentData)))
		if err != nil {
			return nil, err
		}
	}

	// write key
	result.Write([]byte(currentKey))

	if tomb == 0 {
		// write value
		result.Write(currentData)
	}

	return result.Bytes(), nil
}
