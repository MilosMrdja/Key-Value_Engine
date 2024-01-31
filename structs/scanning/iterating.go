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

func minFind(arrPos []*hashmem.Memtable, arrValues []datatype.DataType) []*hashmem.Memtable {
	sort.Slice(arrValues[:], func(i, j int) bool {
		return arrValues[i].GetChangeTime().After(arrValues[j].GetChangeTime())
	})
	minArray := make([]*hashmem.Memtable, 0)
	minArray = append(minArray, arrPos[0])
	for i := 1; i < len(arrValues); i++ {
		if arrValues[i].GetKey() > arrValues[i-1].GetKey() {
			break
		} else {
			minArray = append(minArray, arrPos[i])
		}
	}
	return minArray
}
func adjustPositions(mapa map[*hashmem.Memtable]datatype.DataType, iterator *iterator.Iterator) datatype.DataType {
	keys := make([]*hashmem.Memtable, 0, len(mapa))
	values := make([]datatype.DataType, 0, len(mapa))

	for key := range mapa {
		keys = append(keys, key)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		a := mapa[keys[i]]
		b := mapa[keys[j]]
		return a.GetKey() < b.GetKey()
	})

	for _, k := range keys {
		values = append(values, mapa[k])
	}
	minArray := minFind(keys, values)
	for _, k := range minArray {
		iterator.IncrementMemTablePosition(*k)
	}
	return mapa[minArray[0]]
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

/*
	l0	[125,65,200,269]  [125,150,200,269]
	l1  [0,0,0,0]  [1250,1500,2000,2690]
	l2  [0,0,0,0]  [12500,15000,20000,26900]



		aa


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
func PrefixIterateSSTable(prefix string, compress1, compress2, oneFile bool) {
	Levels, _ := ioutil.ReadDir("./DataSStable")
	SSTable.ReadIndex("./DataSStable/L0/sstable1/Summary.bin", true, true, 1, false)
	for i := 0; i < len(Levels); i++ {
		ssTemp, _ := ioutil.ReadDir("./DataSStable/L" + strconv.Itoa(i))
		for j := len(ssTemp) - 1; j >= 0; j-- {
			if oneFile {
				continue
			} else {
				prvi, poslednji, _ := SSTable.GetSummaryMinMax("./DataSStable/L"+strconv.Itoa(i)+"/sstable"+strconv.Itoa(j+1)+"/Summary.bin", compress1, compress2, oneFile)
				if prefix < prvi.GetKey() || prefix > poslednji.GetKey() {
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
