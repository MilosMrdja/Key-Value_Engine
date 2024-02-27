package wal_implementation

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sstable/SSTableStruct/SSTable"

	"strconv"
	"strings"
	"time"

	"github.com/edsrzf/mmap-go"
)

type WriteAheadLog struct {
	Segments             []string //list of segments that are loaded in upon the creation of wal
	openedFileWrite      *os.File
	openedFileRead       *os.File
	currentWritePosition int
	currentReadPosition  int
	folderPath           string
	segmentSize          int
}

type CustomError struct {
	message string
}

func (e CustomError) Error() string {
	return e.message
}

const (
	MAXSIZE       = 1000
	SEGMENTS_NAME = "wal_"
	//LOW_WATER_MARK = 5 //index to which segments will be deleted
	HEADER_SIZE = 8 //first 4 bytes is how much of record remains from last segment and last 4 bytes are indicating if this is the last segment (all zeors means its not)

)

func NewWriteAheadLog(SegmentSize int) *WriteAheadLog {
	// Specify the folder path here
	if SegmentSize < MAXSIZE {
		SegmentSize = MAXSIZE
	}
	fp := fmt.Sprintf("wal_implementation%cwal", os.PathSeparator) // ako se promeni mora se i u funkciji goToNextReadFile split promeniti za promenljivu s
	listOfSegments := make([]string, 0)
	files, err := os.ReadDir(fp)
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
		filePath = fmt.Sprintf("%s%c%s%s.log", fp, os.PathSeparator, SEGMENTS_NAME, "00001")
		listOfSegments = append(listOfSegments, fmt.Sprintf("%s%s.log", SEGMENTS_NAME, "00001"))
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0777)
		err = file.Truncate(int64(SegmentSize))
		mmapf, _ := mmap.Map(file, mmap.RDWR, 0)
		defer func(mmapf *mmap.MMap) {
			err := mmapf.Unmap()
			if err != nil {

			}
		}(&mmapf)
		byteArray := make([]byte, HEADER_SIZE)
		binary.LittleEndian.PutUint64(byteArray[:HEADER_SIZE], 0)
		copy(mmapf[0:HEADER_SIZE], byteArray)
		err = mmapf.Flush()
	} else {
		filePath = fmt.Sprintf("%s%c%s", fp, os.PathSeparator, listOfSegments[len(listOfSegments)-1])
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0777)
		mmapf, _ := mmap.Map(file, mmap.RDWR, 0)
		defer func(mmapf *mmap.MMap) {
			err := mmapf.Unmap()
			if err != nil {

			}
		}(&mmapf)
		buffer := make([]byte, HEADER_SIZE/2)
		copy(buffer, mmapf[HEADER_SIZE/2:HEADER_SIZE])
		writingPosition = int(binary.LittleEndian.Uint32(buffer))
	}
	filePath = fmt.Sprintf("%s%c%s%s.log", fp, os.PathSeparator, SEGMENTS_NAME, "00001")
	readingFile, err := os.OpenFile(filePath, os.O_RDONLY, 0777)
	if err != nil {
		log.Fatalln(err)
	}

	return &WriteAheadLog{
		Segments:             listOfSegments,
		openedFileWrite:      file,
		openedFileRead:       readingFile,
		currentWritePosition: writingPosition,
		currentReadPosition:  0,
		folderPath:           fp,
		segmentSize:          SegmentSize,
	}
}

func (wal *WriteAheadLog) Log(key string, value []byte, tombstone bool, timestamp time.Time) error {
	record := NewLogRecord(key, value, tombstone, timestamp)
	err := wal.DirectLog(record)
	if err != nil {
		return err
	}
	return nil
}

func (wal *WriteAheadLog) LogDelete(key string, timestamp time.Time) error {
	record := NewLogRecord(key, []byte(""), true, timestamp)
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
	newSegment := fmt.Sprintf("%s%c%s%s.log", wal.folderPath, os.PathSeparator, SEGMENTS_NAME, logsNumber)
	wal.Segments = append(wal.Segments, fmt.Sprintf("%s%s.log", SEGMENTS_NAME, logsNumber))
	mmapf, err := mmap.Map(wal.openedFileWrite, mmap.RDWR, 0)
	defer func(mmapf *mmap.MMap) {
		err := mmapf.Unmap()
		if err != nil {

		}
	}(&mmapf)
	byteArray := make([]byte, HEADER_SIZE/2)
	binary.LittleEndian.PutUint32(byteArray[:HEADER_SIZE/2], 0)
	copy(mmapf[HEADER_SIZE/2:HEADER_SIZE], byteArray)
	err = mmapf.Flush()
	err = wal.openedFileWrite.Close()
	if err != nil {
		return err
	}
	wal.openedFileWrite, err = os.OpenFile(newSegment, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	err = wal.openedFileWrite.Truncate(int64(wal.segmentSize))
	wal.currentWritePosition = 0
	if err != nil {
		return err
	}
	return nil
}

func (wal *WriteAheadLog) DeleteSegmentsTilWatermark(lowWaterMark int) error {
	lwm := lowWaterMark
	if lwm > len(wal.Segments) {
		lwm = len(wal.Segments)
	}
	err := wal.openedFileRead.Close()
	if err != nil {
		return err
	}
	err = wal.openedFileWrite.Close()
	if err != nil {
		return err
	}
	firstFile := fmt.Sprintf("%s%c%s%s.log", wal.folderPath, os.PathSeparator, SEGMENTS_NAME, "00001")
	for i := 1; i < lwm; i++ {
		s := wal.Segments[i-1]
		parts := strings.Split(s, "_")
		numStr := strings.TrimLeft(parts[1], "0")
		num, err := strconv.Atoi(strings.Split(numStr, ".")[0])
		if err != nil {
			return err
		}
		logsNumber := fmt.Sprintf("%05d", num)
		filePath := fmt.Sprintf("%s%c%s%s.log", wal.folderPath, os.PathSeparator, SEGMENTS_NAME, logsNumber)
		////som, err := os.Stat(filePath)
		////fmt.Println(som, err)

		err = os.Rename(filePath, firstFile)

		if err != nil {
			return err
		}
	}

	// If newpath already exists and is not a directory, Rename replaces it.
	wal.Segments = wal.Segments[lwm-1:]
	newSegments := make([]string, 0)
	for i := 0; i < len(wal.Segments); i++ {
		oldPath := fmt.Sprintf("%s%c%s", wal.folderPath, os.PathSeparator, wal.Segments[i])
		logsNumber := fmt.Sprintf("%05d", i+1)
		newPath := fmt.Sprintf("%s%c%s%s.log", wal.folderPath, os.PathSeparator, SEGMENTS_NAME, logsNumber)
		err = os.Rename(oldPath, newPath)
		if err != nil {
			return err
		}
		newSegments = append(newSegments, fmt.Sprintf("%s%s.log", SEGMENTS_NAME, logsNumber))
	}
	wal.Segments = newSegments
	lastPath := fmt.Sprintf("%s%c%s", wal.folderPath, os.PathSeparator, wal.Segments[len(wal.Segments)-1])
	if err != nil {
		return err
	}
	wal.openedFileWrite, err = os.OpenFile(lastPath, os.O_RDWR, 0777)
	mmapf, _ := mmap.Map(wal.openedFileWrite, mmap.RDWR, 0)
	defer func(mmapf *mmap.MMap) {
		err := mmapf.Unmap()
		if err != nil {

		}
	}(&mmapf)
	buffer := make([]byte, HEADER_SIZE/2)
	copy(buffer, mmapf[HEADER_SIZE/2:HEADER_SIZE])
	wal.currentWritePosition = int(binary.LittleEndian.Uint32(buffer))
	firstPath := fmt.Sprintf("%s%c%s", wal.folderPath, os.PathSeparator, wal.Segments[0])
	wal.openedFileRead, err = os.OpenFile(firstPath, os.O_RDONLY, 0777)
	wal.currentReadPosition = 0
	return nil
}

func (wal *WriteAheadLog) goToNextReadFile() error {
	wal.currentReadPosition = 0
	s := strings.Split(wal.openedFileRead.Name(), string(os.PathSeparator))[2]
	parts := strings.Split(s, "_")
	numStr := strings.TrimLeft(parts[1], "0")
	num, err := strconv.Atoi(strings.Split(numStr, ".")[0])
	if err != nil {
		return err
	}
	logsNumber := fmt.Sprintf("%05d", num+1)
	newSegment := fmt.Sprintf("%s%c%s%s.log", wal.folderPath, os.PathSeparator, SEGMENTS_NAME, logsNumber)
	err = wal.openedFileRead.Close()
	if err != nil {
		return err
	}
	wal.openedFileRead, err = os.OpenFile(newSegment, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	return nil
}

func (wal *WriteAheadLog) readOverflow() []byte {
	mmapf, err := mmap.Map(wal.openedFileRead, mmap.RDONLY, 0)
	if err != nil {
		return nil
	}
	defer func(mmapf *mmap.MMap) {
		err := mmapf.Unmap()
		if err != nil {

		}
	}(&mmapf)
	data := make([]byte, wal.segmentSize-wal.currentReadPosition)
	copy(data, mmapf[wal.currentReadPosition:wal.segmentSize])
	for true {
		err := wal.goToNextReadFile()
		if err != nil {
			return nil
		}
		mmapf, err := mmap.Map(wal.openedFileRead, mmap.RDONLY, 0)
		if err != nil {
			return nil
		}
		defer func(mmapf *mmap.MMap) {
			err := mmapf.Unmap()
			if err != nil {

			}
		}(&mmapf)
		buffer := make([]byte, HEADER_SIZE/2)
		copy(buffer, mmapf[:HEADER_SIZE/2])
		wal.currentReadPosition = int(binary.LittleEndian.Uint32(buffer))
		wal.currentReadPosition += HEADER_SIZE
		newBuffer := make([]byte, wal.currentReadPosition-HEADER_SIZE)
		copy(newBuffer, mmapf[HEADER_SIZE:wal.currentReadPosition])
		data = append(data, newBuffer...)
		if wal.currentReadPosition < wal.segmentSize {
			break
		}

	}
	return data
}

func (wal *WriteAheadLog) ReadRecord() (*LogRecord, string) {
	mmapf, err := mmap.Map(wal.openedFileRead, mmap.RDONLY, 0)
	if err != nil {
		return nil, ""
	}
	defer func(mmapf *mmap.MMap) {
		err := mmapf.Unmap()
		if err != nil {

		}
	}(&mmapf)
	buffer := make([]byte, HEADER_SIZE/2)
	copy(buffer, mmapf[HEADER_SIZE/2:HEADER_SIZE])
	isLastFile := int(binary.LittleEndian.Uint32(buffer))
	firstFile := fmt.Sprintf("%s%c%s%s.log", wal.folderPath, os.PathSeparator, SEGMENTS_NAME, "00001")
	if isLastFile != 0 && isLastFile == wal.currentReadPosition {
		return nil, "NO MORE RECORDS"
	} else if isLastFile == 0 && isLastFile == wal.currentReadPosition && wal.currentWritePosition == 0 && wal.openedFileRead.Name() == firstFile {
		return nil, "NO MORE RECORDS"
	}

	if wal.currentReadPosition == 0 {
		buffer := make([]byte, HEADER_SIZE/2)
		copy(buffer, mmapf[:HEADER_SIZE/2])
		wal.currentReadPosition = int(binary.LittleEndian.Uint32(buffer))
		wal.currentReadPosition += HEADER_SIZE
	}
	endIndex := 37
	buffer = make([]byte, 0)
	if endIndex+wal.currentReadPosition > wal.segmentSize {
		buffer = append(buffer, wal.readOverflow()...)
	} else {
		newBuffer := make([]byte, 37)
		copy(newBuffer, mmapf[wal.currentReadPosition:wal.currentReadPosition+37])
		buffer = append(buffer, newBuffer...)
		wal.currentReadPosition += endIndex
		kSize := binary.BigEndian.Uint64(buffer[21:29])
		vSize := binary.BigEndian.Uint64(buffer[29:37])
		if uint64(wal.currentReadPosition)+kSize+vSize > uint64(wal.segmentSize) {
			buffer = append(buffer, wal.readOverflow()...)
		} else {
			newBuffer := make([]byte, kSize+vSize)
			copy(newBuffer, mmapf[wal.currentReadPosition:uint64(wal.currentReadPosition)+kSize+vSize])
			buffer = append(buffer, newBuffer...)
			wal.currentReadPosition += int(kSize) + int(vSize)
		}
	}
	var r LogRecord
	r.CRC = binary.BigEndian.Uint32(buffer[0:4])
	r.Timestamp = buffer[4:20]
	r.Tombstone = buffer[20]
	r.KeySize = binary.BigEndian.Uint64(buffer[21:29])
	r.ValueSize = binary.BigEndian.Uint64(buffer[29:37])
	r.Key = string(buffer[37 : 37+r.KeySize])
	r.Value = buffer[37+r.KeySize : 37+r.KeySize+r.ValueSize]
	expectedCRC := CRC32(buffer[4:])
	if expectedCRC == r.CRC {
		return &r, ""
	}

	return nil, "CRC FAILED!"
}

func (r *LogRecord) AppendToFile(wal *WriteAheadLog) error {
	// Serialize the LogRecord
	data, err := r.ToBinary()
	mmapf, err := mmap.Map(wal.openedFileWrite, mmap.RDWR, 0)
	defer func(mmapf *mmap.MMap) {
		err := mmapf.Unmap()
		if err != nil {

		}
	}(&mmapf)
	if wal.currentWritePosition == 0 {
		wal.currentWritePosition = HEADER_SIZE
	}
	dataLen := len(data)
	if dataLen+wal.currentWritePosition > wal.segmentSize {
		dataLen = wal.segmentSize - wal.currentWritePosition
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
		err := wal.clearLog()
		if err != nil {
			return err
		}
		wal.currentWritePosition = HEADER_SIZE
		var dataLen int
		dataLen = len(data)
		if dataLen+wal.currentWritePosition > wal.segmentSize {
			dataLen = wal.segmentSize - wal.currentWritePosition
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

func (wal *WriteAheadLog) ReadAllRecords() ([]*LogRecord, error) {
	records := make([]*LogRecord, 0)
	for true {
		rec, err := wal.ReadRecord()
		if err != "" {
			if err == "NO MORE RECORDS" {
				break
			}
		}
		if err != "CRC FAILED!" {
			records = append(records, rec)
		}
	}
	return records, nil
}

func (wal *WriteAheadLog) DeleteMemTable() error {
	pathToFile := fmt.Sprintf("wal_implementation%cEndsOfMemtables.bin", os.PathSeparator)
	memEnds, err := SSTable.DeserializeTuples(pathToFile)
	if err != nil {
		return err
	}

	if len(memEnds) == 0 {
		return nil
	}
	s := strings.Split(memEnds[0].X, string(os.PathSeparator))[2]
	parts := strings.Split(s, "_")
	numStr := strings.TrimLeft(parts[1], "0")
	num, err := strconv.Atoi(strings.Split(numStr, ".")[0])
	minNumber := num

	mfp, err := os.OpenFile(memEnds[0].X, os.O_RDWR, 0777)
	mmapf, err := mmap.Map(mfp, mmap.RDWR, 0)

	defer func(mmapf *mmap.MMap) {
		err := mmapf.Unmap()
		if err != nil {

		}
	}(&mmapf)
	byteArray := make([]byte, HEADER_SIZE/2)
	//fmt.Println(uint32((*memEnds)[minFile]))
	binary.LittleEndian.PutUint32(byteArray[:HEADER_SIZE/2], uint32(memEnds[0].Y)-HEADER_SIZE)
	copy(mmapf[:HEADER_SIZE/2], byteArray)
	err = mmapf.Flush()
	if err != nil {
		return err
	}
	memEnds = memEnds[1:]
	//pomeri sve ostale kljuceve
	for index, s1 := range memEnds {
		s := strings.Split(s1.X, string(os.PathSeparator))[2]
		parts := strings.Split(s, "_")
		numStr := strings.TrimLeft(parts[1], "0")
		num, err := strconv.Atoi(strings.Split(numStr, ".")[0])
		if err != nil {
			return err
		}
		logsNumber := fmt.Sprintf("%05d", num-int(minNumber)+1)
		filePath := fmt.Sprintf("%s%c%s%s.log", wal.folderPath, os.PathSeparator, SEGMENTS_NAME, logsNumber)
		memEnds[index].X = filePath

	}
	_, err = SSTable.SerializeTuples(pathToFile, memEnds)
	if err != nil {
		return err
	}
	err = mfp.Close()
	if err != nil {
		return err
	}
	err = wal.DeleteSegmentsTilWatermark(int(minNumber))
	if err != nil {
		return err
	}

	return nil
}

func (wal *WriteAheadLog) EndMemTable() error {
	pathToFile := fmt.Sprintf("wal_implementation%cEndsOfMemtables.bin", os.PathSeparator)
	memEnds, err := SSTable.DeserializeTuples(pathToFile)
	if err != nil {
		if os.IsNotExist(err) {
			memEnds = make([]SSTable.Tuple, 0)
		} else {
			return err
		}
	}
	memEnds = append(memEnds, SSTable.Tuple{X: wal.openedFileWrite.Name(), Y: int32(wal.currentWritePosition)})

	_, err = SSTable.SerializeTuples(pathToFile, memEnds)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	// Example usage

	wal := NewWriteAheadLog(1000)
	for i := 0; i < 10; i++ {
		key := "kljucnestone" + strconv.Itoa(i)
		value_string := "vrednostneka" + strconv.Itoa(i)
		value := []byte(value_string)
		wal.Log(key, value, false, time.Now())
	}
	err := wal.DeleteSegmentsTilWatermark(5)
	if err != nil {
		fmt.Println(err)
	}
	records, err := wal.ReadAllRecords()
	if err != nil {
		fmt.Println(err)
	}
	for _, rec := range records {
		fmt.Println(rec)
	}
	//fmt.Println(records[14].Key)

	//fmt.Println(err)
	//wal.Log("kljuc3", []byte("vrednost"), true)
	//wal.Log("kljuc2", []byte("vrednost2"), true)
	//rec, err := wal.ReadRecord()
	//rec2, err := wal.ReadRecord()
	//rec3, err := wal.ReadRecord()
	//rec4, err := wal.ReadRecord()
	//fmt.Println(rec2.Key, string(rec2.Value), err, rec)
	//fmt.Println(rec3.Key, string(rec3.Value))
	//fmt.Println(rec4, err)

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
