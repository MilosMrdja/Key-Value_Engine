package scanning

import (
	"io/ioutil"
	"os"
	"sort"
	"sstable/SSTableStruct/SSTable"
	"sstable/iterator"
	"sstable/mem/memtable/datatype"
	"sstable/mem/memtable/hash/hashmem"
	"strconv"
)

func isInRange(value string, valRange [2]string) bool {
	return value >= valRange[0] && value <= valRange[1]
}

func extractMinimalKeysSS(mapa map[string]datatype.DataType) []string {
	// Find the minimal value in the map
	minimalValue := findMinimalValueSS(mapa)

	// Initialize a slice to store keys with minimal value
	var minimalKeys []string

	// Iterate through the map to find keys with minimal value
	for key, value := range mapa {
		if value.GetKey() == minimalValue.GetKey() {
			minimalKeys = append(minimalKeys, key)
		}
	}

	return minimalKeys
}

func extractMinimalKeysMems(mapa map[*hashmem.Memtable]datatype.DataType) []*hashmem.Memtable {
	// Find the minimal value in the map
	minimalValue := findMinimalValueMems(mapa)

	// Initialize a slice to store keys with minimal value
	var minimalKeys []*hashmem.Memtable

	// Iterate through the map to find keys with minimal value
	for key, value := range mapa {
		if value.GetKey() == minimalValue.GetKey() {
			minimalKeys = append(minimalKeys, key)
		}
	}

	return minimalKeys
}

func findMinimalValueSS(mapa map[string]datatype.DataType) datatype.DataType {
	// Find the minimal value in the map
	minValue := datatype.DataType{}
	for _, value := range mapa {
		minValue = value
		break
	}
	for _, value := range mapa {
		if value.GetKey() < minValue.GetKey() {
			minValue = value
		}
	}
	return minValue
}
func findMinimalValueMems(mapa map[*hashmem.Memtable]datatype.DataType) datatype.DataType {
	// Find the minimal value in the map
	minValue := datatype.DataType{}
	for _, value := range mapa {
		minValue = value
		break
	}
	for _, value := range mapa {
		if value.GetKey() < minValue.GetKey() {
			minValue = value
		}
	}
	return minValue
}

//	func adjustPositionsRangeMem(mapa map[*hashmem.Memtable]datatype.DataType, memIterator *iterator.RangeIterator) datatype.DataType {
//		minKeys := extractMinimalKeysMems(mapa)
//		sort.Slice(minKeys[:], func(i, j int) bool {
//			dataType1 := mapa[minKeys[i]]
//			dataType2 := mapa[minKeys[j]]
//			return dataType1.GetChangeTime().After(dataType2.GetChangeTime())
//		})
//		for _, k := range minKeys {
//			memIterator.IncrementMemTablePosition(k)
//		}
//		return mapa[minKeys[0]]
//	}
//
//	func adjustPositionsRangeSS(mapa map[string]datatype.DataType, memIterator *iterator.RangeIterator) datatype.DataType {
//		minKeys := extractMinimalKeysMems(mapa)
//		sort.Slice(minKeys[:], func(i, j int) bool {
//			dataType1 := mapa[minKeys[i]]
//			dataType2 := mapa[minKeys[j]]
//			return dataType1.GetChangeTime().After(dataType2.GetChangeTime())
//		})
//		for _, k := range minKeys {
//			memIterator.IncrementMemTablePosition(k)
//		}
//		return mapa[minKeys[0]]
//	}

func adjustPositionSS(mapa map[string]datatype.DataType) []string {
	minKeys := extractMinimalKeysSS(mapa)
	sort.Slice(minKeys[:], func(i, j int) bool {
		dataType1 := mapa[minKeys[i]]
		dataType2 := mapa[minKeys[j]]
		return dataType1.GetChangeTime().After(dataType2.GetChangeTime())
	})
	//for _, k := range minKeys {
	//	ssIterator.IncrementElementOffset(k, ssIterator.PositionInSSTable[k][2])
	//}
	return minKeys
}
func AdjustPositionRange(mapaMem map[*hashmem.Memtable]datatype.DataType, mapaSS map[string]datatype.DataType, ssIterator *iterator.IteratorRangeSSTable, memIterator *iterator.RangeIterator) (datatype.DataType, bool) {
	var dataType1 datatype.DataType
	var keyMem []*hashmem.Memtable

	var dataType2 datatype.DataType // prazan constructor
	var keySS []string

	if len(mapaMem) != 0 && len(mapaSS) != 0 {
		keyMem = adjustPositionsMem(mapaMem)
		dataType1 = mapaMem[keyMem[0]]

		keySS = adjustPositionSS(mapaSS)
		dataType2 = mapaSS[keySS[0]]
		if dataType1.GetKey() > dataType2.GetKey() {
			for i := 0; i < len(keySS); i++ {
				ssIterator.IncrementElementOffset(keySS[i], ssIterator.PositionInSSTable[keySS[i]][2])
			}
			return dataType2, true
		} else if dataType1.GetKey() < dataType2.GetKey() {
			for i := 0; i < len(keyMem); i++ {
				memIterator.IncrementMemTablePosition(keyMem[i])
			}
			return dataType1, true
		} else {
			for i := 0; i < len(keySS); i++ {
				ssIterator.IncrementElementOffset(keySS[i], ssIterator.PositionInSSTable[keySS[i]][2])
			}
			for j := 0; j < len(keyMem); j++ {
				memIterator.IncrementMemTablePosition(keyMem[j])
			}
			if dataType1.GetChangeTime().After(dataType2.GetChangeTime()) {
				return dataType1, true
			} else {
				return dataType2, true
			}
		}
	} else if len(mapaMem) == 0 && len(mapaSS) != 0 {
		keySS = adjustPositionSS(mapaSS)
		dataType2 = mapaSS[keySS[0]]
		for i := 0; i < len(keySS); i++ {
			ssIterator.IncrementElementOffset(keySS[i], ssIterator.PositionInSSTable[keySS[i]][2])
		}
		return dataType2, true
	} else if len(mapaSS) == 0 && len(mapaMem) != 0 {
		keyMem = adjustPositionsMem(mapaMem)
		dataType1 = mapaMem[keyMem[0]]
		for i := 0; i < len(keyMem); i++ {
			memIterator.IncrementMemTablePosition(keyMem[i])
		}
		return dataType1, true
	} else {
		return datatype.DataType{}, false
	}
}
func AdjustPositionPrefix(mapaMem map[*hashmem.Memtable]datatype.DataType, mapaSS map[string]datatype.DataType, ssIterator *iterator.IteratorPrefixSSTable, memIterator *iterator.PrefixIterator) (datatype.DataType, bool) {

	var dataType1 datatype.DataType
	var keyMem []*hashmem.Memtable

	var dataType2 datatype.DataType // prazan constructor
	var keySS []string

	if len(mapaMem) != 0 && len(mapaSS) != 0 {
		keyMem = adjustPositionsMem(mapaMem)
		dataType1 = mapaMem[keyMem[0]]

		keySS = adjustPositionSS(mapaSS)
		dataType2 = mapaSS[keySS[0]]
		if dataType1.GetKey() > dataType2.GetKey() {
			for i := 0; i < len(keySS); i++ {
				ssIterator.IncrementElementOffset(keySS[i], ssIterator.GetSSTableMap()[keySS[i]][2])
			}
			return dataType2, true
		} else if dataType1.GetKey() < dataType2.GetKey() {
			for i := 0; i < len(keyMem); i++ {
				memIterator.IncrementMemTablePosition(keyMem[i])
			}
			return dataType1, true
		} else {
			for i := 0; i < len(keySS); i++ {
				ssIterator.IncrementElementOffset(keySS[i], ssIterator.GetSSTableMap()[keySS[i]][2])
			}
			for j := 0; j < len(keyMem); j++ {
				memIterator.IncrementMemTablePosition(keyMem[j])
			}
			if dataType1.GetChangeTime().After(dataType2.GetChangeTime()) {
				return dataType1, true
			} else {
				return dataType2, true
			}
		}
	} else if len(mapaMem) == 0 && len(mapaSS) != 0 {
		keySS = adjustPositionSS(mapaSS)
		dataType2 = mapaSS[keySS[0]]
		for i := 0; i < len(keySS); i++ {
			ssIterator.IncrementElementOffset(keySS[i], ssIterator.GetSSTableMap()[keySS[i]][2])
		}
		return dataType2, true
	} else if len(mapaSS) == 0 && len(mapaMem) != 0 {
		keyMem = adjustPositionsMem(mapaMem)
		dataType1 = mapaMem[keyMem[0]]
		for i := 0; i < len(keyMem); i++ {
			memIterator.IncrementMemTablePosition(keyMem[i])
		}
		return dataType1, true
	} else {
		return datatype.DataType{}, false
	}

}

func adjustPositionsMem(mapa map[*hashmem.Memtable]datatype.DataType) []*hashmem.Memtable {
	minKeys := extractMinimalKeysMems(mapa)

	sort.Slice(minKeys[:], func(i, j int) bool {
		dataType1 := mapa[minKeys[i]]
		dataType2 := mapa[minKeys[j]]
		return dataType1.GetChangeTime().After(dataType2.GetChangeTime())
	})
	//for _, k := range minKeys {
	//	memIterator.IncrementMemTablePosition(*k)
	//}
	return minKeys
}
func PrefixIterateSSTable(prefix string, compress1, compress2 bool) *iterator.IteratorPrefixSSTable {

	Levels, _ := ioutil.ReadDir("./DataSSTable")
	mapa := make(map[string][]uint64)
	duzinaPref := len(prefix)
	//SSTable.ReadIndex("./DataSStable/L0/sstable1/Summary.bin", true, true, 1, false)
	for i := 0; i < len(Levels); i++ {
		ssTemp, _ := ioutil.ReadDir("./DataSSTable/L" + strconv.Itoa(i))
		for j := 0; j < len(ssTemp); j++ {
			prvi, poslednji, _ := SSTable.GetSummaryMinMax("./DataSSTable/L"+strconv.Itoa(i)+"/sstable"+strconv.Itoa(j+1), compress1, compress2)
			if duzinaPref <= len(poslednji.GetKey()) {
				if prefix < prvi.GetKey()[:duzinaPref] || prefix > poslednji.GetKey()[:duzinaPref] {
					continue
				} else {
					fileSST := "./DataSSTable/L" + strconv.Itoa(i) + "/sstable" + strconv.Itoa(j+1)
					mapa[fileSST] = GetBeginsEnds(fileSST)
				}
			}

		}
	}
	//for k, v := range mapa {
	//	fmt.Printf("\nKey %s - > %d - %d", k, v[0], v[1])
	//}
	return &iterator.IteratorPrefixSSTable{PositionInSSTable: mapa, Prefix: prefix}
}

// string[0] - string[1]
// preduslov: rang[0] je manje od rang[1]
func RangeIterateSSTable(rang [2]string, compress1, compress2 bool) *iterator.IteratorRangeSSTable {
	Levels, _ := ioutil.ReadDir("./DataSSTable")
	mapa := make(map[string][]uint64)
	//SSTable.ReadIndex("./DataSStable/L0/sstable1/Summary.bin", true, true, 1, false)
	for i := 0; i < len(Levels); i++ {
		ssTemp, _ := ioutil.ReadDir("./DataSSTable/L" + strconv.Itoa(i))
		for j := 0; j < len(ssTemp); j++ {
			prvi, poslednji, _ := SSTable.GetSummaryMinMax("./DataSSTable/L"+strconv.Itoa(i)+"/sstable"+strconv.Itoa(j+1), compress1, compress2)

			if rang[1] < prvi.GetKey() || rang[0] > poslednji.GetKey() {
				continue
			} else {
				fileSST := "./DataSSTable/L" + strconv.Itoa(i) + "/sstable" + strconv.Itoa(j+1)
				mapa[fileSST] = GetBeginsEnds(fileSST)
			}
		}
	}
	//for k, v := range mapa {
	//	fmt.Printf("\nKey %s - > %d - %d", k, v[0], v[1])
	//}
	return &iterator.IteratorRangeSSTable{PositionInSSTable: mapa, Rang: rang}
}

func GetBeginsEnds(sstableFile string) []uint64 {
	beginsEnds := make([]uint64, 2)
	oneFile := SSTable.GetOneFile(sstableFile)
	if oneFile {
		file, err := os.OpenFile(sstableFile+"/SSTable.bin", os.O_RDONLY, 0666)
		if err != nil {
			return nil
		}
		defer file.Close()

		start, end := SSTable.PositionInSSTable(*file, 5)
		beginsEnds[0] = 0
		beginsEnds[1] = uint64(end - start)

	} else {

		fileInfo, err := os.Stat(sstableFile + "/Data.bin")
		if err != nil {
			panic(err)
		}
		beginsEnds[0] = 0
		beginsEnds[1] = uint64(fileInfo.Size())

	}
	beginsEnds = append(beginsEnds, 0)
	return beginsEnds
}
