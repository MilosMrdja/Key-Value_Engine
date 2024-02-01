package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sstable/LSM"
	"sstable/cursor"
	"sstable/iterator"
	"sstable/lru"
	"sstable/mem/memtable/hash/hashmem"
	"sstable/mem/memtable/hash/hashstruct"
	"sstable/scanning"
	"sstable/token_bucket"
	"sstable/wal_implementation"
	"strconv"
	"time"
)

var compress1 bool
var compress2 bool
var oneFile bool
var number, lruCap int
var N int
var M int
var memTableCap, memTableNumber int
var memType, compType string
var walSegmentSize int
var rate, maxToken int64
var key, value string

type Config struct {
	LruCap         int    `json:"lru_cap"`
	Compress1      bool   `json:"compress1"`
	Compress2      bool   `json:"compress2"`
	OneFile        bool   `json:"oneFile"`
	Number         int    `json:"numberOfSSTable"`
	N              int    `json:"N"` // razudjenost u indexu
	M              int    `json:"M"` // razudjenost u summary
	MemTableNumber int    `json:"memTableNumber"`
	MemTableCap    int    `json:"memTableCap"`
	MemType        string `json:"memType"`
	WalSegmentSize int    `json:"walSegmentSize"`
	Rate           int64  `json:"rate"`
	MaxToken       int64  `json:"maxToken"`
	CompType       string `json:"compType"`
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
	lruCap = config.LruCap
	compress1 = config.Compress1
	compress2 = config.Compress2
	oneFile = config.OneFile
	number = config.Number
	N = config.N
	M = config.M
	memTableNumber = config.MemTableNumber
	memTableCap = config.MemTableCap
	memType = config.MemType
	walSegmentSize = config.WalSegmentSize
	rate = config.Rate
	maxToken = config.MaxToken
	compType = config.CompType

}
func GET(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, memtable *cursor.Cursor, key string) {
	////ukoliko je GET
	value, ok := memtable.GetElement(key)
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
func PUT(wal *wal_implementation.WriteAheadLog, memtable *cursor.Cursor, key string, value []byte) {

	//Prvo u WAL
	timestamp := time.Now()
	err := wal.Log(key, value, false, timestamp)
	if err != nil {
		panic(err)
	}
	//Drugo u mem

	ok := memtable.AddToMemtable(key, value, timestamp, wal)
	if !ok {
		panic("Greska")
	}
	// kada je put ne ide u LRU
}

func DELETE(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, memtable *cursor.Cursor, key string) {
	//Ukoliko je unos DELETE
	timestamp := time.Now()
	err := wal.LogDelete(key, timestamp)
	if err != nil {
		panic(err)
	}
	ok := memtable.DeleteElement(key, timestamp)
	if ok {
		fmt.Printf("Obrisan")
	} else {
		//zapis se dodaje u memtable kao nov sa detele na true
		ok = memtable.AddToMemtable(key, []byte(""), timestamp, wal)
		fmt.Printf("Obrisan ")
	}

	lru1.Delete(key)
}

func meni(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, memtable *cursor.Cursor, tokenb *token_bucket.TokenBucket) {
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
			PUT(wal, memtable, key, []byte(value))
		} else if opcija == "2" {
			fmt.Printf("Unesite key : ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			DELETE(wal, lru1, memtable, key)
		} else if opcija == "3" {
			fmt.Printf("Unesite key : ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			GET(wal, lru1, memtable, key)
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

func scantest() {
	var mapMem map[*hashmem.Memtable]int
	prefix := "1"
	mapMem = make(map[*hashmem.Memtable]int)

	j := 0

	for i := 0; i < 5; i++ {
		btm := hashmem.Memtable(hashstruct.CreateHashMemtable(15))
		for k := 0; k < 14; k++ {
			btm.AddElement(strconv.Itoa(k), []byte(strconv.Itoa(k)), time.Now())

		}
		btm.SendToSSTable(compress1, compress2, oneFile, 2, 3)

	}
	j = 17
	for i := 0; i < 5; i++ {
		btm := hashmem.Memtable(hashstruct.CreateHashMemtable(10))
		for k := 0; k < 10; k++ {
			btm.AddElement(strconv.Itoa(j), []byte(strconv.Itoa(j)), time.Now())
			j++
		}

		mapMem[&btm] = 0
	}
	iterMem := iterator.NewPrefixIterator(mapMem, prefix)
	iterSSTable := scanning.PrefixIterateSSTable(prefix, compress2, compress1, oneFile)
	scanning.PREFIX_SCAN_OUTPUT(prefix, 1, 10, iterMem, iterSSTable, compress1, compress2, oneFile)

	for k, _ := range mapMem {
		mapMem[k] = 0
	}
	j = 0
	valRange := [2]string{"1", "2"}
	iterMemR := iterator.NewRangeIterator(mapMem, valRange)
	iterSSTableR := scanning.RangeIterateSSTable(valRange, compress2, compress1, oneFile)
	scanning.RANGE_SCAN_OUTPUT(valRange, 1, 10, iterMemR, iterSSTableR, compress1, compress2, oneFile)
	fmt.Println("")
}

func main() {
	setConst()

	//kreiranje potrebnih instanci
	wal := wal_implementation.NewWriteAheadLog(walSegmentSize)
	tokenb := token_bucket.NewTokenBucket(rate, maxToken)
	tokenb.InitRequestsFile("token_bucket/requests.bin")
	lru1 := lru.NewLRUCache(lruCap)

	memtable := cursor.NewCursor(memType, memTableNumber, lru1, compress1, compress2, oneFile, N, M, number, memTableCap, compType)
	memtable.Fill(wal)
	meni(wal, lru1, memtable, tokenb)

	//scantest()
	//wal := wal_implementation.NewWriteAheadLog(walSegmentSize)
	//for i := 0; i < 1000; i++ {
	//	key := "kljuc" + strconv.Itoa(i)
	//	value_string := "vrednost" + strconv.Itoa(i)
	//	value := []byte(value_string)
	//	err := wal.Log(key, value, false, time.Now())
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//	if i%100 == 0 && i != 0 {
	//		err := wal.EndMemTable()
	//		if err != nil {
	//			return
	//		}
	//	}
	//}
	//err := wal.DeleteMemTable()
	//if err != nil {
	//	fmt.Println(err)
	//}
	//err = wal.DeleteMemTable()
	//if err != nil {
	//	fmt.Println(err)
	//}
	////wal.DeleteSegmentsTilWatermark()
	//records, err := wal.ReadAllRecords()
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println(len(records))
	//for _, rec := range records {
	//	fmt.Println(rec)
	//}

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

	//compress1 := true
	//compress2 := true
	//oneFile := true
	//N := 1
	//M := 1
	//memTableCap := 10
	//
	//m := 10
	//var mapMem map[*hashmem.Memtable]int
	//prefix := "1"
	//mapMem = make(map[*hashmem.Memtable]int)
	//
	//for i := 0; i < 2; i++ {
	//	btmem := hashmem.Memtable(hashstruct.CreateHashMemtable(m))
	//
	//	for j := 0; j < 10; j++ {
	//		btmem.AddElement(strconv.Itoa(j+10), []byte(strconv.Itoa(j)))
	//	}
	//	mapMem[&btmem] = 0
	//	btmem.DeleteElement(strconv.Itoa(15))
	//	btmem.SendToSSTable(compress1, compress2, oneFile, N, M)
	//	//SSTable.ReadIndex("DataSSTable/L0/sstable"+strconv.Itoa(i+1), compress1, compress2, 2, oneFile)
	//	//SSTable.ReadIndex("DataSSTable/L0/sstable"+strconv.Itoa(i+1), compress1, compress2, 3, oneFile)
	//	LSM.CompactSstable(10, compress1, compress2, oneFile, N, M, memTableCap, "level")
	//
	//}
	//
	//LSM.CompactSstable(10, compress1, compress2, oneFile, N, M, memTableCap, "level")
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
	//Ne brisi, iter test

	//dataType = scanning.PREFIX_ITERATE(prefix, iterMem, iterSSTable, compress1, compress2, oneFile)
	//fmt.Println(dataType)
	//dataType = scanning.PREFIX_ITERATE(prefix, iterMem, iterSSTable, compress1, compress2, oneFile)
	//fmt.Println(dataType)
	//dataType = scanning.PREFIX_ITERATE(prefix, iterMem, iterSSTable, compress1, compress2, oneFile)
	//fmt.Println(dataType)
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
