package main

import (
	"fmt"
	"sstable/LSM"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/btree/btreemem"
	"strconv"
)

func main() {
	j := 10
	//mem := hashstruct.CreateHashMemtable(j)
	//mem.AddElement("11", []byte("1"))
	//mem.AddElement("2", []byte("2"))
	//mem.AddElement("3", []byte("3"))
	//mem.AddElement("4", []byte("4"))
	//mem.AddElement("7", []byte("7"))
	//mem.AddElement("8", []byte("8"))
	//mem.AddElement("9", []byte("9"))
	//mem.AddElement("5", []byte("5"))
	//mem.AddElement("6", []byte("6"))
	//mem.AddElement("10", []byte("10"))
	////datalist := mem.SendToSSTable()
	//slmem := skiplistmem.CreateSkipListMemtable(j)
	//slmem.AddElement("11", []byte("1"))
	//slmem.AddElement("2", []byte("2"))
	//slmem.AddElement("3", []byte("3"))
	//slmem.AddElement("4", []byte("4"))
	//slmem.AddElement("7", []byte("7"))
	//slmem.AddElement("8", []byte("8"))
	//slmem.AddElement("9", []byte("9"))
	//slmem.AddElement("5", []byte("5"))
	//slmem.AddElement("6", []byte("6"))
	//slmem.AddElement("10", []byte("10"))
	//
	////datalist = slmem.SendToSSTable()
	//
	//btmem := btreemem.NewBTreeMemtable(j)
	//btmem.AddElement("11", []byte("1"))
	//btmem.AddElement("22", []byte("2"))
	//btmem.AddElement("3", []byte("3"))
	//btmem.AddElement("4", []byte("4"))
	//btmem.AddElement("7", []byte("7"))
	//btmem.AddElement("8", []byte("8"))
	//btmem.AddElement("9", []byte("9"))
	//btmem.AddElement("5", []byte("5"))
	//btmem.AddElement("6", []byte("6"))
	//btmem.AddElement("10", []byte("10"))

	//found, elem := btmem.GetElement("10")
	//if found {
	//	fmt.Printf("%s \n", elem)
	//}
	//
	//found, elem = btmem.GetElement("100001")
	//if found {
	//	fmt.Printf("%s \n", elem)
	//}

	//datalist = btmem.SendToSSTable()
	//for i := 0; i < len(datalist); i++ {
	//	fmt.Printf("%s ", datalist[i].GetData())
	//	fmt.Printf("\n")
	//}

	//key := "1"

	//btmem.DeleteElement("10")

	//btmem.SendToSSTable(compres, oneFile)
	//data, err4 := SSTable.GetData("DataSSTable/L0/sstable1", key, compres, oneFile)
	//if err4 == true {
	//	fmt.Printf("Key: %s\n", data.GetKey())
	//	fmt.Printf("Value: %s\n", data.GetData())
	//	fmt.Printf("Time: %s\n", data.GetChangeTime())
	//} else {
	//	fmt.Printf("Ne postoji podatak sa kljucem %s", key)
	//}

	//key := "9"

	//btmem.DeleteElement("22")
	//btmem.SendToSSTable(compress1, compress2, oneFile)
	compress1 := true
	compress2 := true
	oneFile := true
	for i := 0; i < 1000; i++ {
		btmem := btreemem.NewBTreeMemtable(j)
		for j = 0; j < 10; j++ {
			btmem.AddElement(strconv.Itoa(j+i), []byte(strconv.Itoa(j+i)))
		}
		btmem.SendToSSTable(compress1, compress2, oneFile)
		LSM.CompactSstable(10, compress1, compress2, oneFile)
	}
	LSM.CompactSstable(10, compress1, compress2, oneFile)
	//SSTable.ReadIndex("DataSSTableCompact/Summary.bin", "", compress1, compress2, 1, oneFile)
	////SSTable.ReadIndex("DataSSTableCompact/Index.bin", "", compress1, compress2, 2, oneFile)
	fmt.Printf("Konacna: \n")
	SSTable.ReadSSTable("DataSSTable/L2/sstable1", compress1, compress2, oneFile)
	////data, err4 := SSTable.GetData("DataSSTableCompact", key, compres, oneFile)
	//if err4 == true {
	//	fmt.Printf("Key: %s\n", data.GetKey())
	//	fmt.Printf("Value: %s\n", data.GetData())
	//	fmt.Printf("Time: %s\n", data.GetChangeTime())
	//} else {
	//	fmt.Printf("Ne postoji podatak sa kljucem %s", key)
	//}

}