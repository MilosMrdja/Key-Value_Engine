package main

import (
	"fmt"
	"sstable/LSM"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/btree/btreemem"
	"strconv"
)

func main() {
	//wal := wal_implementation.NewWriteAheadLog()
	//mem1 := btreemem.NewBTreeMemtable(10)
	//for i := 0; i < 10; i++ {
	//	err := wal.Log(strconv.Itoa(i), []byte(strconv.Itoa(i)), false)
	//	if err != nil {
	//		panic(err)
	//	}
	//	mem1.AddElement(strconv.Itoa(i), []byte(strconv.Itoa(i)))
	//}

	// conf
	compress1 := false
	compress2 := true
	oneFile := false

	m := 10
	for i := 0; i < 10; i++ {
		btmem := btreemem.NewBTreeMemtable(m)
		for j := 0; j < 10; j++ {
			btmem.AddElement(strconv.Itoa(j+i), []byte(strconv.Itoa(j+i)))
		}
		btmem.SendToSSTable(compress1, compress2, oneFile)
		LSM.CompactSstable(10, compress1, compress2, oneFile)
	}
	LSM.CompactSstable(10, compress1, compress2, oneFile)
	//SSTable.ReadIndex("DataSSTableCompact/Summary.bin", "", compress1, compress2, 1, oneFile)
	////SSTable.ReadIndex("DataSSTableCompact/Index.bin", "", compress1, compress2, 2, oneFile)
	fmt.Printf("Konacna: \n")
	SSTable.ReadSSTable("DataSSTable/L1/sstable1", compress1, compress2, oneFile)
	key := "17"

	SSTable.ReadIndex("DataSSTable/L1/sstable1/Summary.bin", key, compress1, compress2, 1, oneFile)
	SSTable.ReadIndex("DataSSTable/L1/sstable1/Index.bin", key, compress1, compress2, 1, oneFile)
	data, err4 := SSTable.GetData("DataSStable/L1/sstable1", key, compress1, compress2, oneFile)
	if err4 == true {
		fmt.Printf("Key: %s\n", data.GetKey())
		fmt.Printf("Value: %s\n", data.GetData())
		fmt.Printf("Time: %s\n", data.GetChangeTime())
	} else {
		fmt.Printf("Ne postoji podatak sa kljucem %s", key)
	}

}
