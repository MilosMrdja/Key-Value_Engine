package memtable

import "time"

type DataType struct {
	data       []byte
	delete     byte
	changeTime time.Time
}

func CreateDataType(data []byte) *DataType {
	return &DataType{
		data:       data,
		delete:     0x00,
		changeTime: time.Now(),
	}
}

func (dt *DataType) UpdateDataType(data []byte) {
	dt.data = data
	dt.changeTime = time.Now()
}

func (dt *DataType) DeleteDataType() {
	dt.delete = 0x01
	dt.changeTime = time.Now()
}

func (dt *DataType) IsDeleted() bool {
	if dt.delete == 0x01 {
		return true
	}
	return false
}
