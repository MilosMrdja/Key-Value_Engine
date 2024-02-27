package SSTable

import "os"

func GetOffsetStartEnd(compSSTable *map[string][]int64) {

	var offset []int64
	var start, end int64
	var elem int
	elem = 5
	var oneFile bool
	for path, _ := range *compSSTable {
		offset = make([]int64, 0)
		oneFile = GetOneFile(path)
		if oneFile {
			file, err := os.OpenFile(path+"/SSTable.bin", os.O_RDONLY, 0666)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			start, end = PositionInSSTable(*file, elem)
			offset = append(offset, start)
			offset = append(offset, end)
			(*compSSTable)[path] = offset
		} else {
			fileInfo, err := os.Stat(path + "/Data.bin")
			if err != nil {
				panic(err)
			}
			start = 0
			end = fileInfo.Size()
			offset = append(offset, start)
			offset = append(offset, end)
			(*compSSTable)[path] = offset
		}
	}

}
