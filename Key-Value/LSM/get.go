package LSM

import (
	"os"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/datatype"
	"strconv"
)

func GetByKey(key string, compress1, compress2, oneFile bool) (datatype.DataType, bool) {
	dataDir, err := os.Open("./DataSStable")
	if err != nil {
		panic(err)
	}
	err = dataDir.Close()
	if err != nil {
		panic(err)
	}
	layerNames, err := dataDir.Readdirnames(-1)
	var data datatype.DataType
	for i := 0; i < len(layerNames); i++ {
		filelayer, err := os.Open(dataDir.Name() + "/" + layerNames[i])
		if err != nil {
			panic(err)
		}
		err = filelayer.Close()
		if err != nil {
			panic(err)
		}
		sstableName, errNames := filelayer.Readdirnames(-1)
		if errNames != nil {
			panic(errNames)
		}
		for j := len(sstableName) - 1; j >= 0; j-- {
			data, greska := SSTable.GetData("./DataSStable"+"/L"+strconv.Itoa(i)+"/"+sstableName[j], key, compress1, compress2)
			if data.GetKey() == key {
				return data, greska
			} else if greska && data.GetKey() == "" {
				return data, true
			}
		}
	}
	return data, false
}

func GetDataByPrefix(number *int, prefix string, compress1, compress2, oneFile bool) ([]datatype.DataType, string, int64, bool) {
	dataDir, err := os.Open("./DataSStable")
	if err != nil {
		panic(err)
	}
	err = dataDir.Close()
	if err != nil {
		panic(err)
	}
	layerNames, err := dataDir.Readdirnames(-1)
	var data []datatype.DataType
	for i := 0; i < len(layerNames); i++ {
		filelayer, err := os.Open(dataDir.Name() + "/" + layerNames[i])
		if err != nil {
			panic(err)
		}
		err = filelayer.Close()
		if err != nil {
			panic(err)
		}
		sstableName, errNames := filelayer.Readdirnames(-1)
		if errNames != nil {
			panic(errNames)
		}
		for j := len(sstableName) - 1; j >= 0; j-- {
			data, path, offset, greska := SSTable.GetByPrefix("./DataSStable"+"/L"+strconv.Itoa(i)+"/"+sstableName[j], prefix, compress1, compress2, number)
			return data, path, offset, greska
		}
	}
	return data, "", 0, false
}
func GetDataByRange(number *int, valrange []string, compress1, compress2, oneFile bool) ([]datatype.DataType, string, int64, bool) {
	dataDir, err := os.Open("./DataSStable")
	if err != nil {
		panic(err)
	}
	err = dataDir.Close()
	if err != nil {
		panic(err)
	}
	layerNames, err := dataDir.Readdirnames(-1)
	var data []datatype.DataType
	for i := 0; i < len(layerNames); i++ {
		filelayer, err := os.Open(dataDir.Name() + "/" + layerNames[i])
		if err != nil {
			panic(err)
		}
		err = filelayer.Close()
		if err != nil {
			panic(err)
		}
		sstableName, errNames := filelayer.Readdirnames(-1)
		if errNames != nil {
			panic(errNames)
		}
		for j := len(sstableName) - 1; j >= 0; j-- {
			data, path, offset, greska := SSTable.GetByRange("./DataSStable"+"/L"+strconv.Itoa(i)+"/"+sstableName[j], valrange, compress1, compress2, number)
			return data, path, offset, greska
		}
	}
	return data, "", 0, false
}
