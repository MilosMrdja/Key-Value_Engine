package LSM

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
)

func FindNextDestination(layer int) (string, bool) {

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

	newSstableName := "./DataSStable/L" + strconv.Itoa(layer) + "/sstable" + string(strconv.Itoa(len(layerEntries)+1))
	errMkdir := os.Mkdir(newSstableName, os.ModePerm)
	if errMkdir != nil {
		panic(err)
	}
	return newSstableName, false
}

func CompactSstable(numTables int, compres, oneFile bool) {

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

		if len(sstableName) >= numTables*int(math.Pow10(i)) {
			if _, err := os.Stat(dataDir.Name() + "/L" + strconv.Itoa(i+1)); errors.Is(err, os.ErrNotExist) {
				err := os.Mkdir(dataDir.Name()+"/L"+strconv.Itoa(i+1), os.ModePerm)
				if err != nil {
					panic(err)
				}
			}
			newSstableName, _ := FindNextDestination(i + 1)
			fmt.Println(newSstableName)
			createSstableNextLayer(newSstableName, dataDir.Name()+"/L"+strconv.Itoa(i), compres, oneFile)
			deleteLayer(dataDir.Name() + "/L" + strconv.Itoa(i))
			createLayer(dataDir.Name() + "/L" + strconv.Itoa(i))
		}

	}
}
func createSstableNextLayer(newSstableName, oldFilePath string, compres, oneFile bool) {
	//SSTable.NewSSTableCompact(newSstableName, 1, oldFilePath, 1, 1, 10, compres, oneFile)

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

func Compact() {

}

func createLayer(layerName string) {
	if _, err := os.Stat(layerName); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(layerName, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
}
