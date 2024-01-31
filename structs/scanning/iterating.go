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

func extractMinimalKeys(mapa map[*hashmem.Memtable]datatype.DataType) []*hashmem.Memtable {
	// Find the minimal value in the map
	minimalValue := findMinimalValue(mapa)

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

func findMinimalValue(mapa map[*hashmem.Memtable]datatype.DataType) datatype.DataType {
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
func adjustPositionsRange(mapa map[*hashmem.Memtable]datatype.DataType, iterator *iterator.RangeIterator) datatype.DataType {
	minKeys := extractMinimalKeys(mapa)
	sort.Slice(minKeys[:], func(i, j int) bool {
		dataType1 := mapa[minKeys[i]]
		dataType2 := mapa[minKeys[j]]
		return dataType1.GetChangeTime().After(dataType2.GetChangeTime())
	})
	for _, k := range minKeys {
		iterator.IncrementMemTablePosition(*k)
	}
	return mapa[minKeys[0]]
}
func adjustPositionsPrefix(mapa map[*hashmem.Memtable]datatype.DataType, iterator *iterator.PrefixIterator) datatype.DataType {
	minKeys := extractMinimalKeys(mapa)
	sort.Slice(minKeys[:], func(i, j int) bool {
		dataType1 := mapa[minKeys[i]]
		dataType2 := mapa[minKeys[j]]
		return dataType1.GetChangeTime().After(dataType2.GetChangeTime())
	})
	for _, k := range minKeys {
		iterator.IncrementMemTablePosition(*k)
	}
	return mapa[minKeys[0]]
}
func RANGE_ITERATE(valueRange []string, iterator *iterator.RangeIterator) {
	if iterator.ValRange()[0] == valueRange[0] || iterator.ValRange()[1] == valueRange[1] {
		iterator.SetValRange(valueRange)
		iterator.ResetMemTableIndexes()
	}
	minMap := make(map[*hashmem.Memtable]datatype.DataType)
	for i := range iterator.MemTablePositions() {
		for {
			if i.GetMaxSize() == iterator.MemTablePositions()[i] {
				break

			} else if !isInRange(i.GetSortedDataTypes()[iterator.MemTablePositions()[i]].GetKey(), iterator.ValRange()) {
				iterator.MemTablePositions()[i] = i.GetMaxSize()
				break
			} else if isInRange(i.GetSortedDataTypes()[iterator.MemTablePositions()[i]].GetKey(), iterator.ValRange()) {
				minMap[&i] = i.GetSortedDataTypes()[iterator.MemTablePositions()[i]]
				break
			} else {
				iterator.MemTablePositions()[i]++
			}
		}
	}
	minInMems := adjustPositionsRange(minMap, iterator)
	fmt.Println(minInMems)
}

// za memtabelu
func PREFIX_ITERATE(prefix string, iterator *iterator.PrefixIterator) {
	if prefix != iterator.CurrPrefix() {
		iterator.SetCurrPrefix(prefix)
		iterator.ResetMemTableIndexes()
	}
	minMap := make(map[*hashmem.Memtable]datatype.DataType)
	for i := range iterator.MemTablePositions() {
		for {
			if i.GetMaxSize() == iterator.MemTablePositions()[i] {
				break
			} else if !strings.HasPrefix(i.GetSortedDataTypes()[iterator.MemTablePositions()[i]].GetKey(), iterator.CurrPrefix()) && i.GetSortedDataTypes()[iterator.MemTablePositions()[i]].GetKey() > iterator.CurrPrefix() {
				iterator.MemTablePositions()[i] = i.GetMaxSize()
				break
			} else if strings.HasPrefix(i.GetSortedDataTypes()[iterator.MemTablePositions()[i]].GetKey(), iterator.CurrPrefix()) {
				minMap[&i] = i.GetSortedDataTypes()[iterator.MemTablePositions()[i]]
				break
			} else {
				iterator.MemTablePositions()[i]++
			}
		}
	}
	minInMems := adjustPositionsPrefix(minMap, iterator)
	fmt.Println(minInMems)
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
func PrefixIterateSSTable(prefix string, compress1, compress2, oneFile bool) *iterator.IteratorSSTable {

	Levels, _ := ioutil.ReadDir("./DataSStable")
	mapa := make(map[string][]int64)
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
	return &iterator.IteratorSSTable{PositionInSSTable: mapa, Prefix: prefix}
}

func GetBeginsEnds(sstableFile string, oneFile bool) []int64 {
	beginsEnds := make([]int64, 2)
	if oneFile {
		file, err := os.OpenFile(sstableFile+"/SSTable.bin", os.O_RDONLY, 0666)
		if err != nil {
			return nil
		}
		defer file.Close()

		start, end := SSTable.PositionInSSTable(*file, 5)
		beginsEnds[0] = 0
		beginsEnds[1] = end - start

	} else {

		fileInfo, err := os.Stat(sstableFile + "/Data.bin")
		if err != nil {
			panic(err)
		}
		beginsEnds[0] = 0
		beginsEnds[1] = fileInfo.Size()

	}
	return beginsEnds
}
