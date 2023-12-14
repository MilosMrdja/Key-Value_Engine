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

func addStartCommit(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, "<START>")
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.BigEndian, data)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.BigEndian, "<COMMIT>")
	if err != nil {
		return nil, err
	}
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
		data, err = addStartCommit(data)
		if err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}
	err = file.Truncate(currentLen + int64(len(data)))
	if err != nil {
		return err
	}
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()
	copy(mmapf[currentLen:], data)
	err = mmapf.Flush()
	if err != nil {
		return err
	}
	return nil
}

func DeserializeLogSegment(file *os.File) ([]*LogRecord, error) {
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	if fileInfo.Size() == 0 {
		return make([]*LogRecord, 0), nil
	}
	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmapf.Unmap()
	allRecords := make([]*LogRecord, 0)
	startIndex := 0
	endIndex := 37
	for endIndex < len(mmapf) {
		var r LogRecord
		buffer := make([]byte, endIndex-startIndex)
		copy(buffer, mmapf[startIndex:endIndex])
		r.CRC = binary.BigEndian.Uint32(buffer[0:4])
		r.Timestamp = buffer[4:20]
		r.Tombstone = buffer[20]
		r.KeySize = binary.BigEndian.Uint64(buffer[21:29])
		r.ValueSize = binary.BigEndian.Uint64(buffer[29:37])
		buffer = make([]byte, r.KeySize)
		startIndex += 37
		endIndex += int(int64(r.KeySize))
		copy(buffer, mmapf[startIndex:endIndex])
		r.Key = string(buffer)
		startIndex += int(int64(r.KeySize))
		endIndex += int(int64(r.ValueSize))
		buffer = make([]byte, r.ValueSize)
		copy(buffer, mmapf[startIndex:endIndex])
		r.Value = buffer
		if CRC32(r.Value) == r.CRC {
			allRecords = append(allRecords, &r)
		}
		startIndex += int(int64(r.ValueSize))
		endIndex += 37
	}
	return allRecords, nil
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
