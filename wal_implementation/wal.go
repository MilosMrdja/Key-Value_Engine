package main

import (
	"encoding/binary"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"log"
	"os"
	"strconv"
	"strings"
)

type WriteAheadLog struct {
	Segments   []string //list of segments that are loaded in upon the creation of wal
	openedFile *os.File
}

const (
	MAXSIZE        = 10
	SEGMENTS_NAME  = "wal_"
	LOW_WATER_MARK = 500 //index to which segments will be deleted
)

func NewWriteAheadLog() *WriteAheadLog {
	folderPath := "./wal" // Specify the folder path here
	listOfSegments := make([]string, 0)
	files, err := os.ReadDir(folderPath)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if !file.IsDir() {
			listOfSegments = append(listOfSegments, file.Name())
		}
	}
	filePath := ""
	if len(listOfSegments) == 0 {
		filePath = fmt.Sprintf("wal%c%s%s.log", os.PathSeparator, SEGMENTS_NAME, "00001")
		listOfSegments = append(listOfSegments, fmt.Sprintf("%s%s.log", SEGMENTS_NAME, "00001"))
	} else {
		filePath = fmt.Sprintf("wal%c%s", os.PathSeparator, listOfSegments[len(listOfSegments)-1])
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		log.Fatalln(err)
	}
	return &WriteAheadLog{
		Segments:   listOfSegments,
		openedFile: file,
	}
}

func (wal *WriteAheadLog) Log(key string, value []byte, tombstone bool) error {
	record := NewLogRecord(key, value, tombstone)
	err := wal.DirectLog(record)
	if err != nil {
		return err
	}
	return nil
}

func (wal *WriteAheadLog) DirectLog(record *LogRecord) error {
	//to do segmentation by bytes
	var err error
	wal.openedFile, err = record.AppendToFile(wal.openedFile)
	if err != nil {
		return err
	}
	return nil
}

func (wal *WriteAheadLog) clearLog() error {
	s := wal.Segments[len(wal.Segments)-1]
	parts := strings.Split(s, "_")
	numStr := strings.TrimLeft(parts[1], "0")
	num, err := strconv.Atoi(strings.Split(numStr, ".")[0])
	if err != nil {
		return err
	}
	logsNumber := fmt.Sprintf("%05d", num+1)
	newSegment := fmt.Sprintf("wal%c%s%s.log", os.PathSeparator, SEGMENTS_NAME, logsNumber)
	wal.Segments = append(wal.Segments, newSegment)
	err = wal.openedFile.Close()
	if err != nil {
		return err
	}
	wal.openedFile, err = os.OpenFile(newSegment, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (wal *WriteAheadLog) DeleteSegmentsTilWatermark() error {
	lwm := LOW_WATER_MARK
	if lwm > len(wal.Segments) {
		lwm = len(wal.Segments)
	}
	for i := 1; i < lwm; i++ {
		s := wal.Segments[i-1]
		parts := strings.Split(s, "_")
		numStr := strings.TrimLeft(parts[1], "0")
		num, err := strconv.Atoi(strings.Split(numStr, ".")[0])
		if err != nil {
			return err
		}
		logsNumber := fmt.Sprintf("%05d", num)
		filePath := fmt.Sprintf("wal%c%s%s.log", os.PathSeparator, SEGMENTS_NAME, logsNumber)
		err = os.Remove(filePath)
		if err != nil {
			return err
		}
	}
	wal.Segments = wal.Segments[lwm-1:]
	if len(wal.Segments) == 1 {
		err := wal.openedFile.Close()
		if err != nil {
			return err
		}
		oldPath := fmt.Sprintf("wal%c%s", os.PathSeparator, wal.Segments[0])
		logsNumber := fmt.Sprintf("%05d", 1)
		newPath := fmt.Sprintf("wal%c%s%s.log", os.PathSeparator, SEGMENTS_NAME, logsNumber)
		err = os.Rename(oldPath, newPath)
		if err != nil {
			return err
		}
		wal.openedFile, err = os.OpenFile(newPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		wal.Segments = []string{newPath}
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

func (r *LogRecord) AppendToFile(file *os.File) (*os.File, error) {
	// Serialize the LogRecord
	data, err := r.ToBinary()
	currentLen, err := fileLen(file)
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	defer mmapf.Unmap()
	if currentLen == 0 {
		err = file.Truncate(7)
		copy(mmapf[0:7], []byte("<START>"))
		err = mmapf.Flush()
		if err != nil {
			return file, err
		}
	}
	if err != nil {
		return file, err
	}
	if int64(len(data))+currentLen > MAXSIZE {
		if err != nil {
			return file, err
		}
	}

	if err != nil {
		return file, err
	}
	var lenToTruncate int64
	lenToTruncate = int64(len(data))
	if lenToTruncate+currentLen > MAXSIZE {
		lenToTruncate = int64(MAXSIZE) - currentLen
	}
	err = file.Truncate(currentLen + lenToTruncate)
	if err != nil {
		return file, err
	}

	if err != nil {
		return file, err
	}

	copy(mmapf[currentLen:MAXSIZE], data[:lenToTruncate])
	data = data[lenToTruncate:]
	for len(data) > 0 {

	}
	err = mmapf.Flush()
	if err != nil {
		return file, err
	}
	return file, nil
}

func main() {
	// Example usage
	wal := NewWriteAheadLog()
	wal.DeleteSegmentsTilWatermark()

	//fmt.Println(len(wal.LastSegment))
	//fmt.Println(wal.LastSegment)
	key := "mykey"
	value := []byte("myvalue")
	////key1 := "mykey1"
	////value1 := []byte("myvalue1")
	////
	record := NewLogRecord(key, value, false)
	wal.DirectLog(record)
	wal.Log("kljuc", []byte("vrednost"), true)
	//wal.Log(record)
	//wal.Log(record)
	//wal.Log(record)
	//wal.Log(record)
	//record = NewLogRecord("PSOslefajsfh", []byte("posledniji"), false)
	//wal.Log(record)
	//
	//for i := 0; i < len(wal.LastSegment); i++ {
	//	fmt.Println(wal.LastSegment[0].Key)
	//}

	//record1 := NewLogRecord(key1, value1, true)
	//record1.AppendToFile()
	//test := DeserializeLogRecord()
	//fmt.Println(string(test[1].Value))

	//deserialized := DeserializeLogRecord()

	// Prints mykey
	//println(string(deserialized.Key))
}
