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
	//wal := wal_implementation.NewWriteAheadLog()
	//for i := 0; i < 10; i++ {
	//	key := "kljuc" + strconv.Itoa(i)
	//	value_string := "vrednost" + strconv.Itoa(i)
	//	value := []byte(value_string)
	//	wal.Log(key, value, false)
	//}
	//err := wal.DeleteSegmentsTilWatermark()
	//if err != nil {
	//	fmt.Println(err)
	//}
	//records, err := wal.ReadAllRecords()
	//if err != nil {
	//	fmt.Println(err)
	//}
	//for _, rec := range records {
	//	fmt.Println(rec)
	//}

	//conf
	compress1 := false
	compress2 := true
	oneFile := true

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
	key := "1"

	SSTable.ReadIndex("DataSSTable/L1/sstable1/Summary.bin", compress1, compress2, 1, oneFile)
	SSTable.ReadIndex("DataSSTable/L1/sstable1/Index.bin", compress1, compress2, 1, oneFile)
	data, err4 := LSM.GetByKey(key, compress1, compress2, oneFile)
	if err4 == true {
		fmt.Printf("Key: %s\n", data.GetKey())
		fmt.Printf("Value: %s\n", data.GetData())
		fmt.Printf("Time: %s\n", data.GetChangeTime())
	} else {
		fmt.Printf("Ne postoji podatak sa kljucem %s\n", key)
	}

	lista, _, _, _ := LSM.GetDataByPrefix(15, "2", compress1, compress2, oneFile)
	for _, i2 := range lista {
		fmt.Printf("Key: %s ", i2.GetKey())
		fmt.Printf("Value: %s\n", i2.GetData())
	}
	rangeString := [2]string{"1", "2"}
	lista, _, _, _ = LSM.GetDataByRange(15, rangeString, compress1, compress2, oneFile)
	for _, i2 := range lista {
		fmt.Printf("Key: %s ", i2.GetKey())
		fmt.Printf("Value: %s\n", i2.GetData())
	}

	//lru1 := lru.NewLRUCache(3)
	//x1 := datatype.CreateDataType("kljuc1", []byte("vrednost1"))
	//
	//lru1.Put(x1)
	//lru1.Put(datatype.CreateDataType("kljuc2", []byte("vrednost2")))
	//lru1.Put(datatype.CreateDataType("kljuc3", []byte("vrednost3")))
	//lru1.Put(datatype.CreateDataType("kljuc4", []byte("vrednost4")))
	//lru1.Delete("kljuc3")
	//proba := lru1.GetAll()
	//for e := proba.Front(); e != nil; e = e.Next() {
	//	fmt.Println(e.Value.(*datatype.DataType).GetKey())
	//}
	//fmt.Println(config.LruCap)

}
