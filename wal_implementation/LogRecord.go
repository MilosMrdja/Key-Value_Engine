package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
)

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

func (r *LogRecord) AppendToFile(file *os.File) error {
	// Serialize the LogRecord
	data, err := r.ToBinary()
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	if int64(len(data))+currentLen > MAXSIZE {
		if err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}
	var lenToTruncate int64
	lenToTruncate = int64(len(data))
	if lenToTruncate+currentLen > MAXSIZE {
		lenToTruncate = int64(MAXSIZE) - currentLen
	}
	err = file.Truncate(currentLen + lenToTruncate)
	if err != nil {
		return err
	}
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()
	copy(mmapf[currentLen:MAXSIZE], data[:MAXSIZE-currentLen])
	err = mmapf.Flush()
	if err != nil {
		return err
	}
	return nil
}

func NewLogRecord(key string, value []byte, tombstone bool) *LogRecord {
	t := byte(0)
	if tombstone {
		t = 1
	}
	currentTime := time.Now()
	currentTimeBytes := make([]byte, 16)

	// Serialize the current time into the byte slice
	binary.BigEndian.PutUint64(currentTimeBytes[8:], uint64(currentTime.Unix()))

	return &LogRecord{
		CRC:       CRC32(value),
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
