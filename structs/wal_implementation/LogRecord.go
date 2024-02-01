package wal_implementation

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"log"
	"os"
	"time"
)

// sve osim key i value je zajedno 37 bajtova
type LogRecord struct {
	CRC       uint32
	Timestamp []byte
	Tombstone byte
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

func (r *LogRecord) ToBinary() ([]byte, error) {
	var buf bytes.Buffer

	// Write CRC
	err := binary.Write(&buf, binary.BigEndian, r.CRC)
	if err != nil {
		return nil, err
	}

	// Write timestamp
	err = binary.Write(&buf, binary.BigEndian, r.Timestamp)
	if err != nil {
		return nil, err
	}

	// Write tombstone
	buf.WriteByte(r.Tombstone)

	// Write key size
	err = binary.Write(&buf, binary.BigEndian, r.KeySize)
	if err != nil {
		return nil, err
	}

	// Write value size
	err = binary.Write(&buf, binary.BigEndian, r.ValueSize)
	if err != nil {
		return nil, err
	}

	// Write key
	buf.Write([]byte(r.Key))

	// Write value
	buf.Write(r.Value)

	return buf.Bytes(), nil
}

func NewLogRecord(key string, value []byte, tombstone bool, timestamp time.Time) *LogRecord {
	t := byte(0)
	if tombstone {
		t = 1
	}
	currentTime := timestamp
	currentTimeBytes := make([]byte, 16)

	// Serialize the current time into the byte slice
	binary.BigEndian.PutUint64(currentTimeBytes[8:], uint64(currentTime.Unix()))

	var buf bytes.Buffer

	// Write timestamp
	err := binary.Write(&buf, binary.BigEndian, currentTimeBytes)
	if err != nil {
		log.Fatal()
	}

	// Write tombstone
	buf.WriteByte(t)

	// Write key size
	err = binary.Write(&buf, binary.BigEndian, uint64(len(key)))
	if err != nil {
		log.Fatal()
	}

	// Write value size
	err = binary.Write(&buf, binary.BigEndian, uint64(len(value)))
	if err != nil {
		log.Fatal()
	}

	// Write key
	buf.Write([]byte(key))

	// Write value
	buf.Write(value)

	return &LogRecord{
		CRC:       CRC32(buf.Bytes()),
		Timestamp: currentTimeBytes,
		Tombstone: t,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}
}
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
