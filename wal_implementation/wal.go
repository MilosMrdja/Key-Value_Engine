package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/edsrzf/mmap-go"
)

type WriteAheadLog struct {
	Segments             []string //list of segments that are loaded in upon the creation of wal
	openedFileWrite      *os.File
	openedFileRead       *os.File
	currentWritePosition int
	currentReadPosition  int
}

type CustomError struct {
	message string
}

func (e CustomError) Error() string {
	return e.message
}

const (
	MAXSIZE        = 40
	SEGMENTS_NAME  = "wal_"
	LOW_WATER_MARK = 500 //index to which segments will be deleted
	HEADER_SIZE    = 8   //first 4 bytes is how much of record remains from last segment and last 4 bytes are indicating if this is the last segment (all zeors means its not)
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
	var file *os.File
	writingPosition := 0
	if len(listOfSegments) == 0 {
		filePath = fmt.Sprintf("wal%c%s%s.log", os.PathSeparator, SEGMENTS_NAME, "00001")
		listOfSegments = append(listOfSegments, fmt.Sprintf("%s%s.log", SEGMENTS_NAME, "00001"))
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
		err = file.Truncate(MAXSIZE)
		mmapf, _ := mmap.Map(file, mmap.RDWR, 0)
		byteArray := make([]byte, HEADER_SIZE)
		binary.LittleEndian.PutUint64(byteArray[:HEADER_SIZE], 0)
		copy(mmapf[0:HEADER_SIZE], byteArray)
		err = mmapf.Flush()
	} else {
		filePath = fmt.Sprintf("wal%c%s", os.PathSeparator, listOfSegments[len(listOfSegments)-1])
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
		mmapf, _ := mmap.Map(file, mmap.RDWR, 0)
		buffer := make([]byte, HEADER_SIZE/2)
		copy(buffer, mmapf[HEADER_SIZE/2:HEADER_SIZE])
		writingPosition = int(binary.LittleEndian.Uint32(buffer))
	}
	filePath = fmt.Sprintf("wal%c%s%s.log", os.PathSeparator, SEGMENTS_NAME, "00001")
	readingFile, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	return &WriteAheadLog{
		Segments:             listOfSegments,
		openedFileWrite:      file,
		openedFileRead:       readingFile,
		currentWritePosition: writingPosition,
		currentReadPosition:  0,
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
	err = record.AppendToFile(wal)
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
	mmapf, err := mmap.Map(wal.openedFileWrite, mmap.RDWR, 0)
	byteArray := make([]byte, HEADER_SIZE/2)
	binary.LittleEndian.PutUint32(byteArray[:HEADER_SIZE/2], 0)
	copy(mmapf[HEADER_SIZE/2:HEADER_SIZE], byteArray)
	err = mmapf.Flush()
	err = wal.openedFileWrite.Close()
	if err != nil {
		return err
	}
	wal.openedFileWrite, err = os.OpenFile(newSegment, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	err = wal.openedFileWrite.Truncate(MAXSIZE)
	wal.currentWritePosition = 0
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
		err := wal.openedFileWrite.Close()
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
		wal.openedFileWrite, err = os.OpenFile(newPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		wal.Segments = []string{newPath}
	}

	return nil
}

func (wal *WriteAheadLog) goToNextReadFile() error {
	wal.currentReadPosition = 0
	s := wal.openedFileRead.Name()
	parts := strings.Split(s, "_")
	numStr := strings.TrimLeft(parts[1], "0")
	num, err := strconv.Atoi(strings.Split(numStr, ".")[0])
	if err != nil {
		return err
	}
	logsNumber := fmt.Sprintf("%05d", num+1)
	newSegment := fmt.Sprintf("wal%c%s%s.log", os.PathSeparator, SEGMENTS_NAME, logsNumber)
	err = wal.openedFileRead.Close()
	if err != nil {
		return err
	}
	if newSegment == wal.openedFileWrite.Name() {
		wal.openedFileRead = wal.openedFileWrite
		return nil
	}
	wal.openedFileRead, err = os.OpenFile(newSegment, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (wal *WriteAheadLog) ReadRecord() (*LogRecord, error) {
	mmapf, err := mmap.Map(wal.openedFileRead, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmapf.Unmap()
	buffer := make([]byte, HEADER_SIZE/2)
	copy(buffer, mmapf[HEADER_SIZE/2:HEADER_SIZE])
	isLastFile := int(binary.LittleEndian.Uint32(buffer))
	if isLastFile != 0 && isLastFile == wal.currentReadPosition {
		return nil, CustomError{"NO MORE RECORDS"}
	}
	if wal.currentReadPosition == 0 {
		buffer := make([]byte, HEADER_SIZE/2)
		copy(buffer, mmapf[0:HEADER_SIZE/2])
		wal.currentReadPosition = int(binary.LittleEndian.Uint32(buffer))
		wal.currentReadPosition += HEADER_SIZE
	}

	endIndex := 37
	if endIndex+wal.currentReadPosition > MAXSIZE {
		endIndex = MAXSIZE - wal.currentReadPosition
	}
	buffer = make([]byte, endIndex)
	copy(buffer, mmapf[wal.currentReadPosition:wal.currentReadPosition+endIndex])
	wal.currentReadPosition += 37
	for len(buffer) < 37 {
		wal.goToNextReadFile()
		wal.currentReadPosition = HEADER_SIZE
		endIndex = 37 - len(buffer)
		if endIndex+wal.currentReadPosition > MAXSIZE {
			endIndex = MAXSIZE - wal.currentReadPosition
		}
		mmapf, err = mmap.Map(wal.openedFileRead, mmap.RDONLY, 0)
		newBuffer := make([]byte, endIndex)
		copy(newBuffer, mmapf[wal.currentReadPosition:wal.currentReadPosition+endIndex])
		wal.currentReadPosition += endIndex
		buffer = append(buffer, newBuffer...)
	}
	var r LogRecord
	r.CRC = binary.BigEndian.Uint32(buffer[0:4])
	r.Timestamp = buffer[4:20]
	r.Tombstone = buffer[20]
	r.KeySize = binary.BigEndian.Uint64(buffer[21:29])
	r.ValueSize = binary.BigEndian.Uint64(buffer[29:37])
	safeToRead := r.KeySize
	if uint64(wal.currentReadPosition)+safeToRead > MAXSIZE {
		safeToRead = uint64(MAXSIZE - wal.currentReadPosition)
	}
	nBuffer := make([]byte, safeToRead)
	copy(nBuffer, mmapf[wal.currentReadPosition:uint64(wal.currentReadPosition)+safeToRead])
	buffer = append(buffer, nBuffer...)
	wal.currentReadPosition += int(safeToRead)
	for uint64(len(buffer)) < 37+r.KeySize {
		wal.goToNextReadFile()
		wal.currentReadPosition = HEADER_SIZE
		endIndex = 37 + int(r.KeySize) - len(buffer)
		if endIndex+wal.currentReadPosition > MAXSIZE {
			endIndex = MAXSIZE - wal.currentReadPosition
		}
		mmapf, err = mmap.Map(wal.openedFileRead, mmap.RDONLY, 0)
		newBuffer := make([]byte, endIndex)
		copy(newBuffer, mmapf[wal.currentReadPosition:wal.currentReadPosition+endIndex])
		wal.currentReadPosition += endIndex
		buffer = append(buffer, newBuffer...)
	}
	r.Key = string(buffer[37:])
	safeToRead = r.ValueSize
	if uint64(wal.currentReadPosition)+safeToRead > MAXSIZE {
		safeToRead = uint64(MAXSIZE - wal.currentReadPosition)
	}
	nBuffer = make([]byte, safeToRead)
	copy(nBuffer, mmapf[wal.currentReadPosition:uint64(wal.currentReadPosition)+safeToRead])
	buffer = append(buffer, nBuffer...)
	wal.currentReadPosition += int(safeToRead)
	for uint64(len(buffer)) < 37+r.KeySize+r.ValueSize {
		wal.goToNextReadFile()
		wal.currentReadPosition = HEADER_SIZE
		endIndex = 37 + int(r.ValueSize) + int(r.KeySize) - len(buffer)
		if endIndex+wal.currentReadPosition > MAXSIZE {
			endIndex = MAXSIZE - wal.currentReadPosition
		}
		mmapf, err = mmap.Map(wal.openedFileRead, mmap.RDONLY, 0)
		newBuffer := make([]byte, endIndex)
		copy(newBuffer, mmapf[wal.currentReadPosition:wal.currentReadPosition+endIndex])
		wal.currentReadPosition += endIndex
		buffer = append(buffer, newBuffer...)
	}
	r.Value = buffer[37+r.KeySize:]
	if CRC32(r.Value) == r.CRC {
		return &r, nil
	}

	return nil, CustomError{"CRC FAILED!"}
}

func (r *LogRecord) AppendToFile(wal *WriteAheadLog) error {
	// Serialize the LogRecord
	data, err := r.ToBinary()
	mmapf, err := mmap.Map(wal.openedFileWrite, mmap.RDWR, 0)
	defer mmapf.Unmap()
	if wal.currentWritePosition == 0 {
		wal.currentWritePosition = HEADER_SIZE
	}
	dataLen := len(data)
	if dataLen+wal.currentWritePosition > MAXSIZE {
		dataLen = MAXSIZE - wal.currentWritePosition
	}
	copy(mmapf[wal.currentWritePosition:wal.currentWritePosition+dataLen], data[:dataLen])
	wal.currentWritePosition += dataLen
	err = mmapf.Flush()
	if err != nil {
		return err
	}
	if dataLen < len(data) {
		data = data[dataLen:]
	} else {
		data = data[:0]
	}
	for len(data) > 0 {
		wal.clearLog()
		wal.currentWritePosition = HEADER_SIZE
		var dataLen int
		dataLen = len(data)
		if dataLen+wal.currentWritePosition > MAXSIZE {
			dataLen = MAXSIZE - wal.currentWritePosition
		}
		mmapf, err = mmap.Map(wal.openedFileWrite, mmap.RDWR, 0)
		byteArray := make([]byte, HEADER_SIZE/2)
		binary.LittleEndian.PutUint32(byteArray[:HEADER_SIZE/2], uint32(dataLen))
		copy(mmapf[:HEADER_SIZE/2], byteArray)
		copy(mmapf[wal.currentWritePosition:wal.currentWritePosition+dataLen], data[:dataLen])
		wal.currentWritePosition += dataLen
		err = mmapf.Flush()
		if err != nil {
			return err
		}
		if dataLen < len(data) {
			data = data[dataLen:]
		} else {
			data = data[:0]
		}

	}
	byteArray := make([]byte, HEADER_SIZE/2)
	binary.LittleEndian.PutUint32(byteArray[:HEADER_SIZE/2], uint32(wal.currentWritePosition))
	copy(mmapf[HEADER_SIZE/2:HEADER_SIZE], byteArray)
	err = mmapf.Flush()
	return nil
}

func main() {
	// Example usage
	wal := NewWriteAheadLog()
	//wal.Log("kljuc3", []byte("vrednost"), true)
	//wal.Log("kljuc2", []byte("vrednost2"), true)
	rec, err := wal.ReadRecord()
	rec2, err := wal.ReadRecord()
	rec3, err := wal.ReadRecord()
	rec4, err := wal.ReadRecord()
	fmt.Println(rec2.Key, string(rec2.Value), err, rec)
	fmt.Println(rec3.Key, string(rec3.Value))
	fmt.Println(rec4, err)

	//wal.DeleteSegmentsTilWatermark()
	//
	////fmt.Println(len(wal.LastSegment))
	////fmt.Println(wal.LastSegment)
	//key := "mykey"
	//value := []byte("myvalue")
	////////key1 := "mykey1"
	////////value1 := []byte("myvalue1")
	////////
	//record := NewLogRecord(key, value, false)
	//wal.DirectLog(record)
	//wal.Log("kljuc", []byte("vrednost"), true)
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
