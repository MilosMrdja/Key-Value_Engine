package hashmem

import (
	"bufio"
	"log"
	"os"
	"scanning/mem/memtable/datatype"
	"strconv"
	"strings"
)

type Memtable interface {
	AddElement(key string, data []byte) bool
	GetElement(key string) (bool, []byte)
	DeleteElement(key string) bool
	SendToSSTable(compress1, compress2, oneFile bool) bool
	IsReadOnly() bool
	GetElementByPrefix(prefix string) []*datatype.DataType
}

// funkcija koja cita podatke iz config fajla i vraca tip memtable i nje podrazumevani kapacitet
func LoadConfig(filePath string) (string, int) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	success := scanner.Scan()
	if success == false {
		err = scanner.Err()
		log.Fatal(err)
	}
	memType := scanner.Text()
	memType = strings.Split(memType, " ")[1]
	scanner.Scan()

	memCap, _ := strconv.Atoi(strings.Split(scanner.Text(), " ")[1])
	return memType, memCap
}
