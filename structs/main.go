package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sstable/LSM"
	"sstable/SSTableStruct/SSTable"
	"sstable/iterator"
	"sstable/lru"
	"sstable/mem/memtable/hash/hashmem"
	"sstable/mem/memtable/hash/hashstruct"
	"sstable/scanning"
	"sstable/token_bucket"
	"sstable/wal_implementation"
	"strconv"
)

var compress1 bool
var compress2 bool
var oneFile bool
var number int
var N int
var M int
var memTableCap int
var memType string
var walSegmentSize int
var key, value string
var rate, maxToken int64

type Config struct {
	LruCap         int    `json:"lru_cap"`
	Compress1      bool   `json:"compress1"`
	Compress2      bool   `json:"compress2"`
	OneFile        bool   `json:"oneFile"`
	Number         int    `json:"numberOfSSTable"`
	N              int    `json:"N"` // razudjenost u indexu
	M              int    `json:"M"` // razudjenost u summary
	MemTableCap    int    `json:"memTableCap"`
	MemType        string `json:"memType"`
	WalSegmentSize int    `json:"walSegmentSize"`
}

func setConst() {
	var config Config

	configData, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(configData, &config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(config)
	compress1 = config.Compress1
	compress2 = config.Compress2
	oneFile = config.OneFile
	number = config.Number
	N = config.N
	M = config.M
	memTableCap = config.MemTableCap
	memType = config.MemType
	walSegmentSize = config.WalSegmentSize

}

func GET(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, mem1 *hashmem.Memtable, key string) {
	////ukoliko je GET
	ok, value := (*mem1).GetElement(key)
	if ok {
		fmt.Printf("Value: %s\n", value)
	}

	value = lru1.Get(key)
	if value != nil {
		fmt.Printf("Value: %s\n", value)
	}

	data, ok := LSM.GetByKey(key, compress1, compress2, oneFile)
	if ok {
		fmt.Printf("Value: %s\n", data.GetData())
	} else {
		fmt.Printf("Nema ga\n")
	}

}

// Ukoliko je unos PUT
func PUT(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, mem1 *hashmem.Memtable, key string, value []byte) {

	//Prvo u WAL
	err := wal.Log(key, value, false)
	if err != nil {
		panic(err)
	}
	//Drugo u mem

	if (*mem1).IsReadOnly() {
		(*mem1).SendToSSTable(compress1, compress2, oneFile, N, M)
	}
	//LSM.CompactSstable(number, compress1, compress2, oneFile)

	ok := (*mem1).AddElement(key, value)
	if !ok {
		panic("Greska")
	}
	// kada je put ne ide u LRU
}

func DELETE(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, mem1 *hashmem.Memtable, key string) {
	//Ukoliko je unos DELETE

	err := wal.Log(key, []byte(""), true)
	if err != nil {
		panic(err)
	}

	ok := (*mem1).DeleteElement(key)
	if ok {
		fmt.Printf("Obrisan iz mem1")
	} else {
		fmt.Printf("Nije u mem1")
	}

	lru1.Delete(key)
}

func meni(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, mem *hashmem.Memtable, tokenb *token_bucket.TokenBucket) {
	for true {
		var opcija string
		fmt.Println("Key-Value Engine")

		fmt.Println("\n1. Put\n2. Delete\n3. Get\n4. Skeniranje\n5. Izlaz\n")
		fmt.Printf("Unesite opciju : ")
		_, err := fmt.Scan(&opcija)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		mess, moze := tokenb.IsRequestAllowed(9)
		if !moze {
			fmt.Printf(mess + "\n")
			continue
		}

		if opcija == "1" {
			fmt.Printf("Unesite key : ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			fmt.Printf("Unesite value : ")
			_, err = fmt.Scan(&value)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			PUT(wal, lru1, mem, key, []byte(value))
		} else if opcija == "2" {
			fmt.Printf("Unesite key : ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			DELETE(wal, lru1, mem, key)
		} else if opcija == "3" {
			fmt.Printf("Unesite key : ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			GET(wal, lru1, mem, key)
		} else if opcija == "4" {
			for true {
				fmt.Println("\n1. Range scan\n2. Prefix Scan\n3. Range iterate\n4. Prefix iterate\n")
				var opcijaSken string
				fmt.Printf("Unesite opciju : ")
				_, err := fmt.Scan(&opcijaSken)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				if opcijaSken == "1" {
					fmt.Printf("range sken")
				} else if opcijaSken == "2" {
					fmt.Printf("pref sken")
				} else if opcijaSken == "3" {
					fmt.Printf("range iter")
				} else if opcijaSken == "4" {
					fmt.Printf("pref sken")
				} else if opcijaSken == "5" {
					fmt.Printf("Izlazak..\n")
					break
				} else {
					fmt.Printf("Izabrali ste pogresnu opcjiu.\n")
				}

			}
		} else if opcija == "5" {
			break
		} else {
			fmt.Printf("Izabrali ste pogresnu opciju.\n")
		}
	}

}

func main() {
	//setConst()
	//wal := wal_implementation.NewWriteAheadLog()
	//rate = 3
	//maxToken = 10
	//tokenb := token_bucket.NewTokenBucket(rate, maxToken)
	//tokenb.InitRequestsFile("token_bucket/requests.bin")
	////mem1 := btreemem.NewBTreeMemtable(10)
	//lru1 := lru.NewLRUCache(3)
	//var mem hashmem.Memtable
	//if memType == "hash" {
	//	mem = hashstruct.CreateHashMemtable(10)
	//} else if memType == "skipl" {
	//	mem = skiplistmem.CreateSkipListMemtable(10)
	//} else {
	//	mem = btreemem.NewBTreeMemtable(10)
	//}
	//
	//meni(wal, lru1, &mem, tokenb)
	//cursor := cursor2.NewCursor(mem, 0, lru1)
	//
	//cursor.MemPointers()[cursor.MemIndex()]
	//
	//Read a single line of input
	//
	//fmt.Println("You entered:", key, value, []byte(value))

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

	compress1 := true
	compress2 := true
	oneFile := true
	N := 1
	M := 1
	memTableCap := 10

	m := 10
	var mapMem map[*hashmem.Memtable]int
	prefix := "1"
	mapMem = make(map[*hashmem.Memtable]int)

	for i := 0; i < 2; i++ {
		btmem := hashmem.Memtable(hashstruct.CreateHashMemtable(m))

		for j := 0; j < 10; j++ {
			btmem.AddElement(strconv.Itoa(j+10), []byte(strconv.Itoa(j)))
		}
		mapMem[&btmem] = 0
		btmem.DeleteElement(strconv.Itoa(15))
		btmem.SendToSSTable(compress1, compress2, oneFile, N, M)
		//SSTable.ReadIndex("DataSSTable/L0/sstable"+strconv.Itoa(i+1), compress1, compress2, 2, oneFile)
		//SSTable.ReadIndex("DataSSTable/L0/sstable"+strconv.Itoa(i+1), compress1, compress2, 3, oneFile)
		LSM.CompactSstable(10, compress1, compress2, oneFile, N, M, memTableCap, "level")

	}

	LSM.CompactSstable(10, compress1, compress2, oneFile, N, M, memTableCap, "level")
	fmt.Printf("Konacna: \n")
	SSTable.ReadSSTable("DataSSTable/L1/sstable1", compress1, compress2, oneFile)
	//SSTable.ReadIndex("DataSSTable/L1/sstable1/Summary.bin", compress1, compress2, 2, oneFile)
	//SSTable.ReadIndex("DataSSTable/L1/sstable1", compress1, compress2, 3, oneFile)
	//key := "9"
	////scanning.PrefixIterateSSTable("ad", false)
	//fmt.Printf("Sumary: ")
	////SSTable.ReadIndex("DataSSTable/L1/sstable1", compress1, compress2, 2, oneFile)
	//data, err4 := LSM.GetByKey(key, compress1, compress2, oneFile)
	//if err4 == true {
	//	fmt.Printf("Key: %s\n", data.GetKey())
	//	fmt.Printf("Value: %s\n", data.GetData())
	//	fmt.Printf("Time: %s\n", data.GetChangeTime())
	//} else {
	//	fmt.Printf("Ne postoji podatak sa kljucem %s\n", key)
	//}
	//Ne brisi, iter test

	btm := hashmem.Memtable(hashstruct.CreateHashMemtable(m))
	for j := 0; j < 10; j++ {
		btm.AddElement(strconv.Itoa(j), []byte(strconv.Itoa(j)))
	}
	mapMem[&btm] = 0
	iterMem := iterator.NewPrefixIterator(mapMem, prefix)
	iterSSTable := scanning.PrefixIterateSSTable(prefix, compress2, compress1, oneFile)
	dataType := scanning.PREFIX_ITERATE(prefix, iterMem, iterSSTable, compress1, compress2, oneFile)
	fmt.Println(dataType)
	dataType = scanning.PREFIX_ITERATE(prefix, iterMem, iterSSTable, compress1, compress2, oneFile)
	fmt.Println(dataType)
	dataType = scanning.PREFIX_ITERATE(prefix, iterMem, iterSSTable, compress1, compress2, oneFile)
	fmt.Println(dataType)
	dataType = scanning.PREFIX_ITERATE(prefix, iterMem, iterSSTable, compress1, compress2, oneFile)
	fmt.Println(dataType)
	//kraj
	//fmt.Printf("Konacna: \n")
	//SSTable.ReadSSTable("DataSSTable/L1/sstable1", compress1, compress2, oneFile)
	//SSTable.ReadIndex("DataSSTable/L1/sstable1/Summary.bin", compress1, compress2, 2, oneFile)
	//SSTable.ReadIndex("DataSSTable/L1/sstable1", compress1, compress2, 3, oneFile)
	//key := "9"
	////scanning.PrefixIterateSSTable("ad", false)
	//fmt.Printf("Sumary: ")
	////SSTable.ReadIndex("DataSSTable/L1/sstable1", compress1, compress2, 2, oneFile)
	//data, err4 := LSM.GetByKey(key, compress1, compress2, oneFile)
	//if err4 == true {
	//	fmt.Printf("Key: %s\n", data.GetKey())
	//	fmt.Printf("Value: %s\n", data.GetData())
	//	fmt.Printf("Time: %s\n", data.GetChangeTime())
	//} else {
	//	fmt.Printf("Ne postoji podatak sa kljucem %s\n", key)
	//}
	//lista, _, _, _ := LSM.GetDataByPrefix(15, "2", compress1, compress2, oneFile)
	//for _, i2 := range lista {
	//	fmt.Printf("Key: %s ", i2.GetKey())
	//	fmt.Printf("Value: %s\n", i2.GetData())
	//}

}
