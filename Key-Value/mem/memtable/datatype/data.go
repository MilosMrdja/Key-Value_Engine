package datatype

import "time"

type DataType struct {
	key        string
	data       []byte
	delete     bool
	changeTime time.Time
}

func (dt *DataType) GetKey() string {
	return dt.key
}

func (dt *DataType) SetKey(key string) {
	dt.key = key
}

func (dt *DataType) GetData() []byte {
	return dt.data
}

func (dt *DataType) SetData(data []byte) {
	dt.data = data
}

func (dt *DataType) GetDelete() bool {
	return dt.delete
}

func (dt *DataType) SetDelete(delete bool) {
	dt.delete = delete
}

func (dt *DataType) GetChangeTime() time.Time {
	return dt.changeTime
}

func (dt *DataType) SetChangeTime(changeTime time.Time) {
	dt.changeTime = changeTime
}

func CreateDataType(key string, data []byte, time time.Time) *DataType {
	var del bool
	if string(data) == "" {
		del = true
	} else {
		del = false
	}
	return &DataType{
		key:        key,
		data:       data,
		delete:     del,
		changeTime: time,
	}
}

func (dt *DataType) UpdateDataType(data []byte, time time.Time) {
	dt.data = data
	dt.changeTime = time
}

func (dt *DataType) DeleteDataType(time time.Time) {
	dt.delete = true
	dt.changeTime = time
}

func (dt *DataType) IsDeleted() bool {
	if dt.delete == true {
		return true
	}
	return false
}
