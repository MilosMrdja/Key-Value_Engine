package LSM

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/datatype"
	"strconv"
	"strings"
)

func FindNextDestination(layer, maxSSTlevel int) (string, bool) {

	if layer == maxSSTlevel {
		layer -= 1
	}
	if _, err := os.Stat("./DataSStable/L" + strconv.Itoa(layer)); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir("./DataSStable/L"+strconv.Itoa(layer), os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
	layerEntries, err := os.ReadDir("./DataSStable/L" + strconv.Itoa(layer))
	if err != nil {
		panic(err)
	}

	newSstableName := "./DataSStable/L" + strconv.Itoa(layer) + "/sstable" + strconv.Itoa(len(layerEntries)+1)
	errMkdir := os.Mkdir(newSstableName, os.ModePerm)
	if errMkdir != nil {
		panic(err)
	}
	return newSstableName, false
}

func CompactSstable(numTables int, probability_bf float64, compres1, compres2, oneFile bool, N, M, memtableCap int, compType string, maxSSTlevel, levelPlus int) {

	//ovako za gore u entrijim
	dataDir, err := os.Open("./DataSStable")
	if err != nil {
		panic(err)
	}
	err = dataDir.Close()
	if err != nil {
		return
	}
	layerNames, err := dataDir.Readdirnames(-1)
	var compSSTable map[string][]int64
	if compType == "size" {

		compSSTable = make(map[string][]int64)

		for i, name := range layerNames {
			filelayer, err := os.Open(dataDir.Name() + "/" + name)
			if err != nil {
				return
			}
			err = filelayer.Close()
			if err != nil {
				return
			}
			sstableName, errNames := filelayer.Readdirnames(-1)
			if errNames != nil {
				panic(errNames)
			}

			if len(sstableName) >= numTables && i < maxSSTlevel-1 {
				if i+1 < maxSSTlevel {
					if _, err := os.Stat(dataDir.Name() + "/L" + strconv.Itoa(i+1)); errors.Is(err, os.ErrNotExist) {
						err := os.Mkdir(dataDir.Name()+"/L"+strconv.Itoa(i+1), os.ModePerm)
						if err != nil {
							panic(err)
						}
					}
				}

				newSstableName, _ := FindNextDestination(i+1, maxSSTlevel)
				fmt.Println(newSstableName)
				//maksimalan broj elemenata u novoj SSTabeli
				maxElemSize := memtableCap * int(math.Pow(float64(numTables), float64(i)))
				for j := 0; j < len(sstableName); j++ {
					a := make([]int64, 2)
					compSSTable[dataDir.Name()+"/L"+strconv.Itoa(i)+"/"+sstableName[j]] = a
				}
				SSTable.GetOffsetStartEnd(&compSSTable)
				SSTable.NewSSTableCompact(newSstableName, compSSTable, probability_bf, N, M, maxElemSize, compres1, compres2, oneFile)
				fmt.Printf("%d", maxElemSize)
			}

		}
	} else if compType == "level" {

		for i, name := range layerNames {
			filelayer, err := os.Open(dataDir.Name() + "/" + name)
			if err != nil {
				return
			}
			err = filelayer.Close()
			if err != nil {
				return
			}
			sstableName, errNames := filelayer.Readdirnames(-1)
			if errNames != nil {
				panic(errNames)
			}

			if len(sstableName) >= numTables*int(math.Pow(float64(levelPlus), float64(i))) && i < maxSSTlevel-1 {
				//jedna tabela sa prethodnog novoa + ostale tabele sa narednog nivoa
				randSST := rand.Intn(len(sstableName)-1) + 1
				minData, maxData, _ := SSTable.GetSummaryMinMax(dataDir.Name()+"/L"+strconv.Itoa(i)+"/sstable"+strconv.Itoa(randSST), compres1, compres2)

				compSSTable = GetSSTableLevelComp(minData, maxData, dataDir.Name()+"/L"+strconv.Itoa(i+1), compres1, compres2, oneFile)
				if compSSTable == nil {
					compSSTable = make(map[string][]int64)
				}
				a := make([]int64, 2)
				compSSTable[dataDir.Name()+"/L"+strconv.Itoa(i)+"/sstable"+strconv.Itoa(randSST)] = a
				SSTable.GetOffsetStartEnd(&compSSTable)

				newSstableName, _ := FindNextDestination(i+1, maxSSTlevel)
				maxElemSize := memtableCap * int(math.Pow(10, float64(i))) * numTables
				fmt.Printf("%d", maxElemSize)
				fmt.Println(newSstableName)
				SSTable.NewSSTableCompact(newSstableName, compSSTable, probability_bf, N, M, maxElemSize, compres1, compres2, oneFile)

			}

		}
	}

	for path, _ := range compSSTable {
		err = os.RemoveAll(path)
		for err != nil {
			err = os.RemoveAll(path)
		}
	}
	RenameSSTable(numTables)

}

func GetSSTableLevelComp(minData, maxData datatype.DataType, filePath string, compres1, compres2, oneFile bool) map[string][]int64 {
	var compSSTable map[string][]int64
	compSSTable = make(map[string][]int64)

	filelayer, err := os.Open(filePath)
	if err != nil {
		return compSSTable
	}
	err = filelayer.Close()
	if err != nil {
		return compSSTable
	}
	sstableName, errNames := filelayer.Readdirnames(-1)
	if errNames != nil {
		panic(errNames)
	}

	for _, name := range sstableName {
		currentMin, currentMax, _ := SSTable.GetSummaryMinMax(filePath+"/"+name, compres1, compres2)
		if currentMin.GetKey() <= minData.GetKey() && currentMax.GetKey() >= minData.GetKey() {

			a := make([]int64, 2)
			compSSTable[filePath+"/"+name] = a

		}
		if minData.GetKey() <= currentMin.GetKey() && maxData.GetKey() >= currentMax.GetKey() {

			a := make([]int64, 2)
			compSSTable[filePath+"/"+name] = a

		}
		if currentMin.GetKey() <= maxData.GetKey() && currentMax.GetKey() >= maxData.GetKey() {
			a := make([]int64, 2)
			compSSTable[filePath+"/"+name] = a
		}
	}
	return compSSTable
}

func deleteLayer(layerName string) {
	file, err := os.Open(layerName)
	if err != nil {
		panic(err)
	}
	dirNames, _ := file.Readdirnames(-1)
	for _, name := range dirNames {
		err := os.RemoveAll(layerName + "/" + name)
		for err != nil {
			err = os.RemoveAll(layerName + "/" + name)
		}
	}
}

func createLayer(layerName string) {
	if _, err := os.Stat(layerName); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(layerName, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
}

func RenameSSTable(numTables int) {
	dataDir, err := os.Open("./DataSStable")
	if err != nil {
		panic(err)
	}
	err = dataDir.Close()
	if err != nil {
		return
	}
	layerNames, err := dataDir.Readdirnames(-1)
	for i, name := range layerNames {
		filelayer, err := os.Open(dataDir.Name() + "/" + name)
		if err != nil {
			return
		}
		err = filelayer.Close()
		if err != nil {
			return
		}
		sstableName, errNames := filelayer.Readdirnames(-1)
		if errNames != nil {
			panic(errNames)
		}
		//preimenovati sve sstabele
		for _, name = range layerNames {
			filelayer, err = os.Open(dataDir.Name() + "/" + name)
			if err != nil {
				panic(err)
			}
			err = filelayer.Close()
			if err != nil {
				panic(err)
			}
			sstableName, errNames = filelayer.Readdirnames(-1)
			if errNames != nil {
				panic(errNames)
			}
			sort.Slice(sstableName, func(i, j int) bool {
				a, err := strconv.ParseInt(strings.Split(sstableName[i], "e")[1], 10, 32)
				if err != nil {
					panic(err)
				}
				b, err := strconv.ParseInt(strings.Split(sstableName[j], "e")[1], 10, 32)
				if err != nil {
					panic(err)
				}
				return a < b
			})

			for i = 0; i < len(sstableName); i++ {
				if sstableName[i] != "sstable"+strconv.Itoa(i+1) {
					err := os.Rename(dataDir.Name()+"/"+name+"/"+sstableName[i], dataDir.Name()+"/"+name+"/"+"sstable"+strconv.Itoa(i+1))
					if err != nil {
						return
					}
				}
			}
		}

	}
}
