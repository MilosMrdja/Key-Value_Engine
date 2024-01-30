package main

import (
	"fmt"
	"sstable/LSM"
	"sstable/SSTableStruct/SSTable"
	"sstable/mem/memtable/btree/btreemem"
	"strconv"
)

func GET() {
	//Funkcija za GET
}

func PUT() {
	//Funkcija za put
}

func DELETE() {
	//Funkcija za DELETE
}

func main() {

	//compress1 := false
	//compress2 := true
	//oneFile := true
	//numberOfSSTable := 10
	//N := 1
	//M := 2
	//memType := "hash"
	//
	//wal := wal_implementation.NewWriteAheadLog()
	//
	//mem1 := btreemem.NewBTreeMemtable(10)
	//lru1 := lru.NewLRUCache(3)
	//var mem []hashmem.Memtable
	//if memType == "hash" {
	//	mem = append(mem, hashstruct.CreateHashMemtable(10))
	//} else if memType == "skipl" {
	//	mem = append(mem, skiplistmem.CreateSkipListMemtable(10))
	//} else {
	//	mem = append(mem, btreemem.NewBTreeMemtable(10))
	//}
	////cursor := cursor2.NewCursor(mem, 0, lru1)
	////
	////cursor.MemPointers()[cursor.MemIndex()]
	//
	////ucitamo sa konzole korisnikov unos
	//
	////Ukoliko je unos PUT
	//key := "kljuc"
	//value := []byte("Ja se zovem Stefan")
	////Prvo u WAL
	//err := wal.Log(key, value, false)
	//if err != nil {
	//	panic(err)
	//}
	////Drugo u mem
	//if mem1.IsReadOnly() {
	//	mem1.SendToSSTable(compress1, compress2, oneFile, N, M)
	//}
	//LSM.CompactSstable(numberOfSSTable, compress1, compress2, oneFile)
	//
	//ok := mem1.AddElement(key, value)
	//if !ok {
	//	panic("Greska")
	//}
	////Trece u LRU
	//lru1.Put(datatype.CreateDataType(key, value))
	//
	////ukoliko je GET
	//key = "kljuc"
	//ok, value = mem1.GetElement(key)
	//if ok {
	//	fmt.Printf("Value: %s\n", value)
	//}
	//
	//value = lru1.Get(key)
	//if value != nil {
	//	fmt.Printf("Value: %s\n", value)
	//}
	//
	//data, ok := LSM.GetByKey(key, compress1, compress2, oneFile)
	//if ok {
	//	fmt.Printf("Value: %s\n", data.GetData())
	//}
	//
	//fmt.Printf("Nema ga\n")
	//
	////Ukoliko je unos DELETE
	//
	//key = "kljuc"
	//err = wal.Log(key, []byte(""), true)
	//if err != nil {
	//	panic(err)
	//}
	//
	//ok = mem1.DeleteElement(key)
	//if ok {
	//	fmt.Printf("Obrisan iz mem1")
	//} else {
	//	fmt.Printf("Nije u mem1")
	//}
	//
	//lru1.Delete(key)

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
	compress1 := true
	compress2 := true
	oneFile := true

	m := 10
	for i := 0; i < 10; i++ {
		btmem := btreemem.NewBTreeMemtable(m)
		for j := 0; j < 10; j++ {
			btmem.AddElement(strconv.Itoa(j+i), []byte(strconv.Itoa(j+i)))
		}
		btmem.DeleteElement(strconv.Itoa(15))
		btmem.SendToSSTable(compress1, compress2, oneFile, 1, 2)
		SSTable.ReadIndex("DataSSTable/L0/sstable"+strconv.Itoa(i+1), compress1, compress2, 1, oneFile)

		LSM.CompactSstable(10, compress1, compress2, oneFile)

	}

	LSM.CompactSstable(10, compress1, compress2, oneFile)
	//SSTable.ReadIndex("DataSSTableCompact/Summary.bin", "", compress1, compress2, 1, oneFile)
	////SSTable.ReadIndex("DataSSTableCompact/Index.bin", "", compress1, compress2, 2, oneFile)
	fmt.Printf("Konacna: \n")
	SSTable.ReadSSTable("DataSSTable/L1/sstable1", compress1, compress2, oneFile)
	key := "1"

	fmt.Printf("SUMM")
	SSTable.ReadIndex("DataSSTable/L1/sstable1", compress1, compress2, 1, oneFile)
	data, err4 := LSM.GetByKey(key, compress1, compress2, oneFile)
	if err4 == true {
		fmt.Printf("Key: %s\n", data.GetKey())
		fmt.Printf("Value: %s\n", data.GetData())
		fmt.Printf("Time: %s\n", data.GetChangeTime())
	} else {
		fmt.Printf("Ne postoji podatak sa kljucem %s\n", key)
	}
	//
	//lista, _, _, _ := LSM.GetDataByPrefix(15, "2", compress1, compress2, oneFile)
	//for _, i2 := range lista {
	//	fmt.Printf("Key: %s ", i2.GetKey())
	//	fmt.Printf("Value: %s\n", i2.GetData())
	//}

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
