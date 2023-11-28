package memtable

import "time"

type DataType struct {
	data       []byte
	delete     bool
	changeTime time.Time
}

func CreateDataType(data []byte) *DataType {
	return &DataType{
		data:       data,
		delete:     true,
		changeTime: time.Now(),
	}
}

func (dt *DataType) UpdateDataType(data []byte) {
	dt.data = data
	dt.changeTime = time.Now()
}

func (dt *DataType) DeleteDataType() {
	dt.delete = true
	dt.changeTime = time.Now()
}

func (dt *DataType) IsDeleted() bool {
	return dt.delete
}
