package testing

import (
	"encoding/binary"
	"sstable/mem/memtable/datatype"
	"strconv"
	"time"
)

func GeneratePopulation(populationSize uint64) []*datatype.DataType {
	rezult := make([]*datatype.DataType, populationSize)
	var i uint64
	for ; i < populationSize; i++ {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, i)
		rezult[i] = datatype.CreateDataType(strconv.FormatUint(i, 10), b, time.Now())
	}
	return rezult
}
