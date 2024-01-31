package scanning

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sstable/SSTableStruct/SSTable"
	"sstable/iterator"
	"sstable/mem/memtable/datatype"
	"sstable/mem/memtable/hash/hashmem"
	"strconv"
	"strings"
)

func isInRange(value string, valRange []string) bool {
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
func adjustPositionsRangeMem(mapa map[*hashmem.Memtable]datatype.DataType, memIterator *iterator.RangeIterator) datatype.DataType {
	minKeys := extractMinimalKeysMems(mapa)
	sort.Slice(minKeys[:], func(i, j int) bool {
		dataType1 := mapa[minKeys[i]]
		dataType2 := mapa[minKeys[j]]
		return dataType1.GetChangeTime().After(dataType2.GetChangeTime())
	})
	for _, k := range minKeys {
		memIterator.IncrementMemTablePosition(k)
	}
	return mapa[minKeys[0]]
}

func adjustPositionPrefixSS(mapa map[string]datatype.DataType, ssIterator *iterator.IteratorPrefixSSTable) []string {
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

func adjustPosition(mapaMem map[*hashmem.Memtable]datatype.DataType, mapaSS map[string]datatype.DataType, ssIterator *iterator.IteratorPrefixSSTable, memIterator *iterator.PrefixIterator) datatype.DataType {
	keyMem := adjustPositionsPrefixMem(mapaMem, memIterator)
	keySS := adjustPositionPrefixSS(mapaSS, ssIterator)

	dataType1 := mapaMem[keyMem[0]]
	dataType2 := mapaSS[keySS[0]]

	if dataType1.GetKey() > dataType2.GetKey() {
		for i := 0; i < len(keySS); i++ {
			ssIterator.IncrementElementOffset(keySS[i], ssIterator.GetSSTableMap()[keySS[i]][2])
		}
	} else if dataType1.GetKey() < dataType2.GetKey() {
		for i := 0; i < len(keyMem); i++ {
			memIterator.IncrementMemTablePosition(keyMem[i])
		}
	} else {
		for i := 0; i < len(keySS); i++ {
			ssIterator.IncrementElementOffset(keySS[i], ssIterator.GetSSTableMap()[keySS[i]][2])
		}
		for j := 0; j < len(keyMem); j++ {
			memIterator.IncrementMemTablePosition(keyMem[j])
		}
	}
	if dataType1.GetChangeTime().After(dataType2.GetChangeTime()) {
		return dataType1
	}
	return dataType2
}

func adjustPositionsPrefixMem(mapa map[*hashmem.Memtable]datatype.DataType, memIterator *iterator.PrefixIterator) []*hashmem.Memtable {
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

func RANGE_ITERATE(valueRange []string, memIterator *iterator.RangeIterator) {
	if memIterator.ValRange()[0] == valueRange[0] || memIterator.ValRange()[1] == valueRange[1] {
		memIterator.SetValRange(valueRange)
		memIterator.ResetMemTableIndexes()
	}
	minMap := make(map[*hashmem.Memtable]datatype.DataType)
	for i := range memIterator.MemTablePositions() {
		for {
			j := *i
			if j.GetMaxSize() == memIterator.MemTablePositions()[i] {
				break

			} else if !isInRange(j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]].GetKey(), memIterator.ValRange()) {
				memIterator.MemTablePositions()[i] = j.GetMaxSize()
				break
			} else if isInRange(j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]].GetKey(), memIterator.ValRange()) {
				minMap[&j] = j.GetSortedDataTypes()[memIterator.MemTablePositions()[i]]
				break
			} else {
				memIterator.MemTablePositions()[i]++
			}
		}
	}
	minInMems := adjustPositionsRangeMem(minMap, memIterator)
	fmt.Println(minInMems)
}

// za memtabelu
func PREFIX_ITERATE(prefix string, memIterator *iterator.PrefixIterator, ssIterator *iterator.IteratorPrefixSSTable, compress1 bool, compress2 bool, oneFile bool) datatype.DataType {
	if prefix != memIterator.CurrPrefix() {
		memIterator.SetCurrPrefix(prefix)
		memIterator.ResetMemTableIndexes()
	}
	if prefix != ssIterator.Prefix {
		ssIterator = PrefixIterateSSTable(prefix, compress1, compress2, oneFile)
	}
	minMap := make(map[*hashmem.Memtable]datatype.DataType)
	for i := range memIterator.MemTablePositions() {
		for {
			j := *i
			if j.GetMaxSize() == memIterator.MemTablePositions()[i] {
				break
			} else if !strings.HasPrefix(j.GetSortedDataTypes()[memIterator.MemTablePositions()[&j]].GetKey(), memIterator.CurrPrefix()) && j.GetSortedDataTypes()[memIterator.MemTablePositions()[&j]].GetKey() > memIterator.CurrPrefix() {
				memIterator.MemTablePositions()[&j] = j.GetMaxSize()
				break
			} else if strings.HasPrefix(j.GetSortedDataTypes()[memIterator.MemTablePositions()[&j]].GetKey(), memIterator.CurrPrefix()) {
				minMap[&j] = j.GetSortedDataTypes()[memIterator.MemTablePositions()[&j]]
				break
			} else {
				memIterator.IncrementMemTablePosition(&j)
			}
		}
	}
	minSsstableMap := make(map[string]datatype.DataType)
	for k, v := range ssIterator.GetSSTableMap() {
		for {
			if ssIterator.GetSSTableMap()[k][0] == ssIterator.GetSSTableMap()[k][1] {
				break
			}
			record, offset := SSTable.GetRecord(k, ssIterator.GetSSTableMap()[k][0], compress1, compress2, oneFile)

			if !strings.HasPrefix(record.GetKey(), ssIterator.Prefix) && record.GetKey() > ssIterator.Prefix {
				v[0] = v[1]
				break
			} else if strings.HasPrefix(record.GetKey(), ssIterator.Prefix) {
				minSsstableMap[k] = record
				v[2] = uint64(offset)
				break
			} else {
				ssIterator.IncrementElementOffset(k, uint64(offset))
			}
		}
	}

	return adjustPosition(minMap, minSsstableMap, ssIterator, memIterator)
}

/*
	l0	[125,65,200,269]  [125,150,200,269]
	l1  [0,0,0,0]  [1250,1500,2000,2690]
	l2  [0,0,0,0]  [12500,15000,20000,26900]

for SVE{
    l0  sst 1    aa
	l0 	sst2      ab
	L1 	SST1      aa

	L0  [[P],[K]]
	L1  [[P],[K]]
	L2  [[P],[K]]
}

lPocM, lKrajaM, elementM    aaj
lPoc, lKraja, element, bool		aab


if elementM < element{
		pokazi elemntM
}
*/

// za sstabelu
func PrefixIterateSSTable(prefix string, compress1, compress2, oneFile bool) *iterator.IteratorPrefixSSTable {

	Levels, _ := ioutil.ReadDir("./DataSStable")
	mapa := make(map[string][]uint64)
	//SSTable.ReadIndex("./DataSStable/L0/sstable1/Summary.bin", true, true, 1, false)
	for i := 0; i < len(Levels); i++ {
		ssTemp, _ := ioutil.ReadDir("./DataSStable/L" + strconv.Itoa(i))
		for j := 0; j < len(ssTemp); j++ {
			prvi, poslednji, _ := SSTable.GetSummaryMinMax("./DataSStable/L"+strconv.Itoa(i)+"/sstable"+strconv.Itoa(j+1), compress1, compress2, oneFile)
			if prefix < prvi.GetKey() || prefix > poslednji.GetKey() {
				continue
			} else {
				fileSST := "./DataSStable/L" + strconv.Itoa(i) + "/sstable" + strconv.Itoa(j+1)
				mapa[fileSST] = GetBeginsEnds(fileSST, oneFile)
			}
		}
	}
	for k, v := range mapa {
		fmt.Printf("\nKey %s - > %d - %d", k, v[0], v[1])
	}
	return &iterator.IteratorPrefixSSTable{PositionInSSTable: mapa, Prefix: prefix}
}

// string[0] - string[1]
// preduslov: rang[0] je manje od rang[1]
func RangeIterateSSTable(rang [2]string, compress1, compress2, oneFile bool) *iterator.IteratorRangeSSTable {
	Levels, _ := ioutil.ReadDir("./DataSStable")
	mapa := make(map[string][]uint64)
	//SSTable.ReadIndex("./DataSStable/L0/sstable1/Summary.bin", true, true, 1, false)
	for i := 0; i < len(Levels); i++ {
		ssTemp, _ := ioutil.ReadDir("./DataSStable/L" + strconv.Itoa(i))
		for j := 0; j < len(ssTemp); j++ {
			prvi, poslednji, _ := SSTable.GetSummaryMinMax("./DataSStable/L"+strconv.Itoa(i)+"/sstable"+strconv.Itoa(j+1), compress1, compress2, oneFile)
			if rang[1] < prvi.GetKey() || rang[0] > poslednji.GetKey() {
				continue
			} else {
				fileSST := "./DataSStable/L" + strconv.Itoa(i) + "/sstable" + strconv.Itoa(j+1)
				mapa[fileSST] = GetBeginsEnds(fileSST, oneFile)
			}
		}
	}
	for k, v := range mapa {
		fmt.Printf("\nKey %s - > %d - %d", k, v[0], v[1])
	}
	return &iterator.IteratorRangeSSTable{PositionInSSTable: mapa, Rang: rang}
}

func GetBeginsEnds(sstableFile string, oneFile bool) []uint64 {
	beginsEnds := make([]uint64, 2)
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
