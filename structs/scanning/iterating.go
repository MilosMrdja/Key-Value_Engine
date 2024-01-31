package scanning

import (
	"fmt"
	"io/ioutil"
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

func adjustPositions(mapa map[*hashmem.Memtable]datatype.DataType, iterator *iterator.Iterator) datatype.DataType {
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

// za memtabelu
func PREFIX_ITERATE(prefix string, iterator *iterator.Iterator) {
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
	minInMems := adjustPositions(minMap, iterator)
	fmt.Println(minInMems)
}

// za sstabelu
func PrefixIterateSSTable(prefix string, oneFile bool) {
	Levels, _ := ioutil.ReadDir("./DataSStable")
	SSTable.ReadIndex("./DataSStable/L0/sstable1/Summary.bin", true, true, 1, false)
	for i := 0; i < len(Levels); i++ {
		ssTemp, _ := ioutil.ReadDir("./DataSStable/L" + strconv.Itoa(i))
		for j := len(ssTemp) - 1; j >= 0; j-- {
			if oneFile {
				continue
			} else {
				prvi, poslednji := SSTable.GetFirstAndLast()
				if prefix < prvi || prefix > poslednji {
					continue
				} else {
					// kreiranje iteratorSSTable
					break
				}
			}
			// Ideja: uzmemo iz summarija prvi i poslednji, ako je manji od prvog ili veci od poslednjeg preskacemo sstabelu
			// ako je dobro uzimamo tu sstabelu sa tog nivoa i prekidamo petlju, idemo na drugi nivo
			// i tako smo postavili sstabele u iterate koje nam trebaju za scan
		}
	}
}
