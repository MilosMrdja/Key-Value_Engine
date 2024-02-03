package SSTable

import (
	"sstable/mem/memtable/datatype"
)

// prva vrednost je min,druga je max
func GetGlobalSummaryMinMax(compSSTable *map[string][]int64, compress1, compress2 bool) (datatype.DataType, datatype.DataType) {
	var minData, maxData datatype.DataType
	for path, _ := range *compSSTable {
		currentMin, currentMax, _ := GetSummaryMinMax(path, compress1, compress2)
		if minData.GetKey() == "" || minData.GetKey() > currentMin.GetKey() {
			minData = currentMin
		}
		if maxData.GetKey() == "" || maxData.GetKey() < currentMax.GetKey() {
			maxData = currentMax
		}
	}

	return minData, maxData
}
