package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sstable/LSM"
	"sstable/MerkleTreeImplementation/MerkleTree"
	"sstable/SSTableStruct/SSTable"
	SimHash "sstable/SimHashImplementation"
	"sstable/bloomfilter/bloomfilter"
	count_min_sketch "sstable/cms"
	"sstable/cursor"
	"sstable/hyperloglog/hyperloglog"
	"sstable/iterator"
	"sstable/lru"
	"sstable/mem/memtable/hash/hashmem"
	"sstable/mem/memtable/hash/hashstruct"
	"sstable/scanning"
	"sstable/token_bucket"
	"sstable/wal_implementation"
	"strconv"
	"strings"
	"time"
)

var compress1 bool
var compress2 bool
var oneFile bool
var NumberOfSST, lruCap int
var N int
var M int
var memTableCap, memTableNumber, levelPlus int
var memType, compType string
var walSegmentSize, maxSSTLevel int
var rate, maxToken int64
var key, value string
var p float64

type Config struct {
	LruCap         int     `json:"lruCap"`
	Compress1      bool    `json:"compress"`
	Compress2      bool    `json:"dictEncoding"`
	OneFile        bool    `json:"oneFile"`
	NumberOfSST    int     `json:"numberOfSSTable"`
	N              int     `json:"indexEl"`   // razudjenost u indexu
	M              int     `json:"summaryEl"` // razudjenost u summary
	MemTableNumber int     `json:"memTableNumber"`
	MemTableCap    int     `json:"memTableCap"`
	MemType        string  `json:"memType"`
	WalSegmentSize int     `json:"walSegmentSize"`
	Rate           int64   `json:"rate"`
	MaxToken       int64   `json:"maxToken"`
	CompType       string  `json:"compType"`
	MaxSSTLevel    int     `json:"maxSSTLevel"`
	LevelPlus      int     `json:"levelPlus"`
	P              float64 `json:"probab"`
}

func setConst() {
	var config Config

	configData, err := os.ReadFile("config.json")
	if err != nil {
		compress1 = false
		compress2 = false
		oneFile = false
		NumberOfSST = 2
		lruCap = 3
		N = 1
		M = 1
		memTableCap = 3
		memTableNumber = 2
		levelPlus = 2
		memType = "hash"
		compType = "level"
		walSegmentSize = 2000
		maxSSTLevel = 5
		rate = 5
		maxToken = 10
		p = 0.01
	}

	err = json.Unmarshal(configData, &config)
	if err != nil {
		log.Fatal(err)
	}
	//provera postojnja u congi.json
	var dataResult map[string]interface{}

	err = json.Unmarshal(configData, &dataResult)
	if err != nil {
		fmt.Println(err)
	}
	// chech for lru
	_, ok := dataResult["lruCap"]
	if ok {
		lruCap = config.LruCap
		if lruCap <= 0 {
			lruCap = 2
		}
	} else {
		lruCap = 2
	}

	// compress1
	_, ok = dataResult["compress"]
	if ok {
		compress1 = config.Compress1
	} else {
		compress1 = false
	}
	//compress2
	_, ok = dataResult["dictEncoding"]
	if ok {
		compress2 = config.Compress2
	} else {
		compress2 = false
	}
	//oneFile
	_, ok = dataResult["oneFile"]
	if ok {
		oneFile = config.OneFile
	} else {
		oneFile = false
	}
	//numOfSST
	_, ok = dataResult["numberOfSSTable"]
	if ok {
		NumberOfSST = config.NumberOfSST
		if NumberOfSST <= 0 {
			NumberOfSST = 3
		}
	} else {
		NumberOfSST = 5
	}

	//memTableNum
	_, ok = dataResult["memTableNumber"]
	if ok {
		memTableNumber = config.MemTableNumber
		if memTableNumber <= 0 {
			memTableNumber = 2
		}
	} else {
		memTableNumber = 2
	}

	//memCap
	_, ok = dataResult["memTableCap"]
	if ok {
		memTableCap = config.MemTableCap
		if memTableCap <= 0 {
			memTableCap = 5
		}
	} else {
		memTableCap = 5
	}
	//indexEl
	_, ok = dataResult["indexEl"]
	if ok {
		N = config.N
	} else {
		N = memTableCap / 3
	}
	if N <= 0 || N >= memTableCap {
		N = 1
	}
	//summaruEl
	_, ok = dataResult["summaryEl"]
	if ok {
		M = config.M
	} else {
		M = memTableCap / 2
	}
	if M <= 0 || M >= memTableCap {
		M = 1
	}
	if M < N {
		M = N + 1
	}

	//memType
	_, ok = dataResult["memType"]
	if ok {
		memType = config.MemType
		if memType != "hash" && memType != "btree" && memType != "skipl" {
			memType = "hash"
		}
	} else {
		memType = "hash"
	}

	//wal
	_, ok = dataResult["walSegmentSize"]
	if ok {
		walSegmentSize = config.WalSegmentSize
	} else {
		walSegmentSize = 2000
	} //provera uradjena u wal-u
	//rateForLRU
	_, ok = dataResult["rate"]
	if ok {
		rate = config.Rate
		if rate <= 0 {
			rate = 2
		}
	} else {
		rate = 3
	}

	//maxToken
	_, ok = dataResult["maxToken"]
	if ok {
		maxToken = config.MaxToken
		if maxToken <= 0 {
			maxToken = 10
		}
	} else {
		maxToken = 10
	}

	//compType
	_, ok = dataResult["compType"]
	if ok {
		compType = config.CompType
		if compType != "size" && compType != "level" {
			compType = "size"
		}
	} else {
		compType = "size"
	}
	//maxSSTVelev
	_, ok = dataResult["maxSSTLevel"]
	if ok {
		maxSSTLevel = config.MaxSSTLevel
		if maxSSTLevel <= 0 {
			maxSSTLevel = 3
		}
	} else {
		maxSSTLevel = 4
	}
	//levelPlus
	_, ok = dataResult["levelPlus"]
	if ok {
		levelPlus = config.LevelPlus
		if levelPlus <= 0 {
			levelPlus = 10
		}
	} else {
		levelPlus = 12
	}

	//prob for hloglog
	_, ok = dataResult["probab"]
	if ok {
		p = config.P
		if p > 0.1 {
			p = 0.01
		}
	} else {
		p = 0.01
	}

}

func checkKey(key string) (bool, string) {
	noKey := [5]string{"bf", "hll", "cms", "sh", ""}
	splitKey := strings.Split(key, "_")
	index := len(splitKey) - 1
	for i := 0; i < len(noKey); i++ {
		if splitKey[index] == noKey[i] {
			return false, noKey[i]
		}
	}
	return true, ""

}

func ValidateSSTable(sstablePath string) {
	fmt.Println("---------------------------------------------------")
	merkleTreePath := SSTable.DeserializeMerkleFromSST(sstablePath)
	merkleTree1, _, err := MerkleTree.DeserializeMerkleTree(merkleTreePath)
	if err != nil {
		panic(err)
	}
	_, merkleTreeByte := SSTable.ReadSSTable(sstablePath, compress1, compress2)
	merkleTree2, _ := MerkleTree.CreateMerkleTree(merkleTreeByte)

	change, _ := MerkleTree.CheckChanges(merkleTree1, merkleTree2)

	if len(change) > 0 {
		fmt.Println("\nIma promene")
		for i := 0; i < len(change); i++ {
			fmt.Printf("Podataka na indexu %d. je promenjen.\n", int(change[i]))
		}
	} else {
		fmt.Println("\nNema promene")
	}

}

func GET(lru1 *lru.LRUCache, memtable *cursor.Cursor, key string) ([]byte, bool) {
	////ukoliko je GET

	ispis, _ := checkKey(key)

	value, ok := memtable.GetElement(key)
	if value != nil {
		if ispis {
			fmt.Printf("Value: %s\n", value)
		}

		return value, true
	} else if string(value) == "" && ok {
		if ispis {
			fmt.Printf("Element sa kljucem %s je obrisan\n", key)

		}
		return value, false
	}

	value = lru1.Get(key)
	if value != nil {
		if ispis {
			fmt.Printf("Value: %s\n", value)
		}
		return value, true
	} else if string(value) == "" && ok {
		if ispis {
			fmt.Printf("Element sa kljucem %s je obrisan\n", key)
		}
		return value, false
	}

	data, ok := LSM.GetByKey(key, compress1, compress2, oneFile)
	if ok && data.GetKey() != "" {
		if ispis {
			fmt.Printf("Value: %s\n", data.GetData())
		}
		return data.GetData(), ok
	} else if data.GetKey() == "" && ok {
		if ispis {
			fmt.Printf("Postoji greska u podacima!\n")
		}
		return data.GetData(), false
	} else {
		if ispis {
			fmt.Printf("Nema ga\n")
		}

		return nil, false
	}

}

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

func TypeBloomFilter(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, memtable *cursor.Cursor) {
	var option string
	for true {
		fmt.Println("\nRad sa BloomFilter tipom: ")
		fmt.Println("\n1. Kreiranje nove instance\n2. Brisanje postojece instance\n3. Dodavanje elementa u postojecu instancu\n4. Provera da li je element u nekoj instanci\n5. Izlaz\n")
		fmt.Printf("Unesite opciju : ")
		_, err := fmt.Scan(&option)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		if option == "1" {
			var key string
			fmt.Println("Unesite kljuc: ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_bf"

			data, found := GET(lru1, memtable, key)
			if found && len(data) != 0 {
				fmt.Printf("Vec postoji element sa tim kljucem!")
			} else {
				var n int
				fmt.Println("Unesite duzinu BloomFilter-a: ")
				_, err := fmt.Scan(&n)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				bf := bloomfilter.CreateBloomFilter(uint64(n), p)
				serializedData, _ := bloomfilter.SerializeBloomFilter(bf)
				PUT(wal, memtable, key, serializedData)
				fmt.Printf("BloomFilter je upisan u sistem!")
			}
		} else if option == "2" {
			var key string
			fmt.Println("Unesite kljuc postojece instance: ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_bf"
			data, found := GET(lru1, memtable, key)
			if !found || len(data) == 0 {
				fmt.Printf("Ne postoji element sa tim kljucem!")
			} else {
				DELETE(wal, lru1, memtable, key)
				fmt.Printf("BloomFilter sa izabranim kljucem je uspesno obrisan!")
			}
		} else if option == "3" {
			var key string
			fmt.Println("Unesite kljuc postojece instance: ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_bf"
			data, found := GET(lru1, memtable, key)
			if !found || len(data) == 0 {
				fmt.Printf("Ne postoji element sa tim kljucem!")
			} else {
				bf, _ := bloomfilter.DeserializeBloomFilter(data)
				var value []byte
				fmt.Println("Unesite vrednost koju unosite u BloomFilter: ")
				_, err := fmt.Scan(&value)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				(*bf).Set(value)
				serializedData, _ := bloomfilter.SerializeBloomFilter(bf)
				PUT(wal, memtable, key, serializedData)
				fmt.Printf("Vrednost je uneta u BloomFilter!")
			}
		} else if option == "4" {
			var key string
			fmt.Println("Unesite kljuc postojece instance: ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_bf"
			data, found := GET(lru1, memtable, key)
			if !found || len(data) == 0 {
				fmt.Printf("Ne postoji element sa tim kljucem!")
			} else {
				bf, _ := bloomfilter.DeserializeBloomFilter(data)
				var value []byte
				fmt.Println("Unesite vrednost koju trazite u BloomFilter-u: ")
				_, err := fmt.Scan(&value)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				found := (*bf).Get(value)
				if !found {
					fmt.Println("Vrednost se sigurno ne nalazi u BloomFilter-u!")
				} else {
					fmt.Println("Vrednost se mozda nalazi u BloomFilter-u!")
				}
			}
		} else if option == "5" {
			fmt.Println("Izlazak...")
			break
		} else {
			fmt.Printf("Uneli ste nepostojecu opciju!")
		}
	}
}

func TypeCountMinSketch(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, memtable *cursor.Cursor) {
	var option string
	for true {
		fmt.Println("\nRad sa CountMinSketch tipom: ")
		fmt.Println("\n1. Kreiranje nove instance\n2. Brisanje postojece instance\n3. Dodavanje dogadjaja u postojecu instancu\n4. Provera ucestalosti dogadjaja u nekoj instanci\n5. Izlaz\n")
		fmt.Printf("Unesite opciju : ")
		_, err := fmt.Scan(&option)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		if option == "1" {
			var key string
			fmt.Println("Unesite kljuc >> ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_cms"
			data, found := GET(lru1, memtable, key)
			if found && len(data) != 0 {
				fmt.Printf("Vec postoji element sa tim kljucem!")
			} else {
				var width, hashes int
				fmt.Println("Unesite sirinu CountMinSketch tabele >>  ")
				_, err := fmt.Scan(&width)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				fmt.Println("Unesite broj HASH funkcija CountMinSketch-a >>  ")
				_, err = fmt.Scan(&hashes)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				cms := count_min_sketch.NewCountMinSketch(width, hashes)
				serializedData, _ := cms.SerializeCountMinSketch()
				PUT(wal, memtable, key, serializedData)
				fmt.Printf("Novi CountMinSketch je uspesno upisan u sistem!")
			}
		} else if option == "2" {
			var key string
			fmt.Println("Unesite kljuc postojece instance >>")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_cms"
			data, found := GET(lru1, memtable, key)
			if !found || len(data) == 0 {
				fmt.Printf("Ne postoji element sa tim kljucem!")
			} else {
				DELETE(wal, lru1, memtable, key)
				fmt.Printf("CountMinSketch sa izabranim kljucem je uspesno obrisan!")
			}
		} else if option == "3" {
			var key string
			fmt.Println("Unesite kljuc postojece instance: ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_cms"
			data, found := GET(lru1, memtable, key)
			if !found || len(data) == 0 {
				fmt.Printf("Ne postoji element sa tim kljucem!")
			} else {
				cms, _ := count_min_sketch.DeserializeCountMinSketch(data)
				var value string
				fmt.Println("Unesite vrednost dogadjaja koji unosite u CountMinSketch: ")
				_, err := fmt.Scan(&value)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				(*cms).Update(value)
				serializedData, _ := (*cms).SerializeCountMinSketch()
				PUT(wal, memtable, key, serializedData)
				fmt.Printf("Vrednost je uneta u CountMinSketch!")
			}
		} else if option == "4" {
			var key string
			fmt.Println("Unesite kljuc postojece instance: ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_cms"
			data, found := GET(lru1, memtable, key)
			if !found || len(data) == 0 {
				fmt.Printf("Ne postoji element sa tim kljucem!")
			} else {
				cms, _ := count_min_sketch.DeserializeCountMinSketch(data)
				var value string
				fmt.Println("Unesite vrednost dogadjaja iz CountMinSketch-a cija vas ucestanost zanima: ")
				_, err := fmt.Scan(&value)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				frequency := (*cms).Estimate(value)
				fmt.Printf("Ucestanost unete vrednosti u CountMinSketch-u: %v", frequency)
			}
		} else if option == "5" {
			break
		} else {
			fmt.Printf("Uneli ste nepostojecu opciju!")
		}
	}
}

func TypeHyperLogLog(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, memtable *cursor.Cursor) {
	var option string
	for true {
		fmt.Println("\nRad sa HyperLogLog tipom: ")
		fmt.Println("\n1. Kreiranje nove instance\n2. Brisanje postojece instance\n3. Dodavanje elementa u postojecu instancu\n4. Provera kardinaliteta\n5. Izlaz\n")
		fmt.Printf("Unesite opciju >> ")
		_, err := fmt.Scan(&option)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		if option == "1" {
			var key string
			fmt.Println("Unesite kljuc >> ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_hll"
			data, found := GET(lru1, memtable, key)
			if found && len(data) != 0 {
				fmt.Printf("Vec postoji element sa tim kljucem!")
			} else {
				var n uint64
				fmt.Println("Unesite duzinu seta HyperLogLog-a >>")
				_, err := fmt.Scan(&n)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				hll := hyperloglog.CreateHyperLogLog(n)
				serializedData, _ := hyperloglog.SerializeHyperLogLog(hll)
				PUT(wal, memtable, key, serializedData)
				fmt.Printf("HyperLogLog je upisan u sistem!")
			}
		} else if option == "2" {
			var key string
			fmt.Println("Unesite kljuc postojece instance >> ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_hll"
			_, found := GET(lru1, memtable, key)
			if !found {
				fmt.Printf("Ne postoji element sa tim kljucem!")
			} else {
				DELETE(wal, lru1, memtable, key)
				fmt.Printf("HyperLogLog sa izabranim kljucem je uspesno obrisan!")
			}
		} else if option == "3" {
			var key string
			fmt.Println("Unesite kljuc postojece instance: ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_hll"
			data, found := GET(lru1, memtable, key)
			if !found || len(data) == 0 {
				fmt.Printf("Ne postoji element sa tim kljucem!")
			} else {
				hll, _ := hyperloglog.DeserializeHyperLogLog(data)
				value := make([]byte, 8)
				var v string
				fmt.Println("Unesite vrednost koju unosite u HyperLogLog: ")

				_, err := fmt.Scan(&v)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}

				value = []byte(v)
				(*hll).Add(value)
				serializedData, _ := hyperloglog.SerializeHyperLogLog(hll)
				hlll, _ := hyperloglog.DeserializeHyperLogLog(serializedData)
				fmt.Println(hlll)
				PUT(wal, memtable, key, serializedData)
				fmt.Printf("Vrednost je uneta u HyperLogLog!")
				fmt.Printf("Kardinalitet izabrane instance HyperLogLog-a: %v", (*hll).CountHLL())
			}
		} else if option == "4" {
			var key string
			fmt.Println("Unesite kljuc postojece instance >>")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_hll"
			data, found := GET(lru1, memtable, key)
			if !found || len(data) == 0 {
				fmt.Printf("Ne postoji element sa tim kljucem!")
			} else {
				hll, _ := hyperloglog.DeserializeHyperLogLog(data)
				fmt.Printf("Kardinalitet izabrane instance HyperLogLog-a: %v", (*hll).CountHLL())
			}
		} else if option == "5" {
			fmt.Println("Izlazak...")
			break
		} else {
			fmt.Printf("Uneli ste nepostojecu opciju!")
		}
	}
}

func TypeSimHash(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, memtable *cursor.Cursor) {
	var option string
	for true {
		fmt.Println("\nRad sa SimHash tipom: ")
		fmt.Println("\n1. Cuvanje fingerprinta prosledjenog teksta\n2. Racunanje Hemingove udaljenosti dva fingerprinta\n3. Izlaz\n")
		fmt.Printf("Unesite opciju : ")
		_, err := fmt.Scan(&option)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		if option == "1" {
			var key string
			fmt.Println("Unesite kljuc >>")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key += "_sh"
			_, found := GET(lru1, memtable, key)
			if found {
				fmt.Printf("Vec postoji element sa tim kljucem!")
			} else {
				var text string
				fmt.Println("Unesite tekst za cuvanje: ")
				_, err := fmt.Scan(&text)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				sh := SimHash.NewSimHash(text)
				value := sh.ReturnIdArray()
				PUT(wal, memtable, key, value)
				fmt.Printf("SimHash je upisan u sistem!")
			}
		} else if option == "2" {
			var key1, key2 string
			fmt.Printf("Unesite kljuceve postojece instance\n>> ")
			_, err := fmt.Scan(&key1)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			fmt.Printf("\n>> ")
			_, err = fmt.Scan(&key2)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key1)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key1 += "_sh"
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr = checkKey(key2)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				return
			}
			key2 += "_sh"
			data1, found1 := GET(lru1, memtable, key1)
			data2, found2 := GET(lru1, memtable, key2)
			if !found1 || !found2 {
				fmt.Printf("Jedan ili oba kljuca ne postoje u sistemu!")
			} else {
				fmt.Printf("Hemingovo rastojanje izmedju dva izabrana fingerprinta: %v", SimHash.HammingDistance(data1, data2))
			}
		} else if option == "3" {
			fmt.Println("Izlazak...")
			break
		} else {
			fmt.Printf("Uneli ste nepostojecu opciju!")
		}
	}
}
func Scan(cursor *cursor.Cursor) {
	for {
		fmt.Println("\n1. Prefix scan\n2. Range Scan\n3. Prefix iterate\n4. Range iterate\n5. Izlazak iz skeniranja")

		var opcijaSken string
		fmt.Printf("Unesite opciju >> ")
		_, err := fmt.Scan(&opcijaSken)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		if opcijaSken == "1" {
			var prefix string
			var strana int
			var brojNaStrani int
			fmt.Println("Unesite preifx >> ")
			_, err = fmt.Scan(&prefix)
			if err != nil {
				panic(err)
			}

			iteratorSSTable := scanning.PrefixIterateSSTable(prefix, compress1, compress2)
			iteratorMem := iterator.NewPrefixIterator(cursor, prefix)

			fmt.Println("Koja stranica po redu: ")
			_, err = fmt.Scan(&strana)
			if err != nil {
				panic(err)
			}

			fmt.Println("Broj zapisa po strani: ")
			_, err = fmt.Scan(&brojNaStrani)
			if err != nil {
				panic(err)
			}

			scanning.PREFIX_SCAN_OUTPUT(prefix, strana, brojNaStrani, iteratorMem, iteratorSSTable, cursor.Compress1(), cursor.Compress2(), cursor.OneFile())

			for true {
				fmt.Println("Da li zelite da vidite prethodnu stranu?")
				var yesOrNo string
				_, err = fmt.Scan(&yesOrNo)
				if err != nil {
					panic(err)
				}
				if yesOrNo == "DA" || yesOrNo == "da" || yesOrNo == "Da" {
					iteratorSSTable = scanning.PrefixIterateSSTable(prefix, compress1, compress2)
					iteratorMem = iterator.NewPrefixIterator(cursor, prefix)
					strana -= 1
					if strana <= 0 {
						fmt.Println("Dosli ste do prve stranice, nema prethodnih vise.")
						break
					}
					scanning.PREFIX_SCAN_OUTPUT(prefix, strana, brojNaStrani, iteratorMem, iteratorSSTable, cursor.Compress1(), cursor.Compress2(), cursor.OneFile())

				} else if yesOrNo == "Ne" || yesOrNo == "NE" || yesOrNo == "ne" {
					fmt.Println("Izlazak...")
					break
				} else {
					fmt.Println("Uneta nepostojeca opcija.")
				}
			}

		} else if opcijaSken == "2" {
			var rangeVal [2]string
			var strana int
			var brojNaStrani int
			fmt.Println("Unesite odakle >> ")
			_, err = fmt.Scan(&rangeVal[0])
			if err != nil {
				panic(err)
			}
			fmt.Println("Unesite dokle >> ")
			_, err = fmt.Scan(&rangeVal[1])
			if err != nil {
				panic(err)
			}

			iteratorSSTable := scanning.RangeIterateSSTable(rangeVal, compress1, compress2)
			iteratorMem := iterator.NewRangeIterator(cursor, rangeVal)

			fmt.Println("Koja stranica po redu: ")
			_, err = fmt.Scan(&strana)
			if err != nil {
				panic(err)
			}

			fmt.Println("Broj zapisa po strani: ")
			_, err = fmt.Scan(&brojNaStrani)
			if err != nil {
				panic(err)
			}

			//pageCache := iterator.NewPageCache(brojNaStrani)
			scanning.RANGE_SCAN_OUTPUT(rangeVal, strana, brojNaStrani, iteratorMem, iteratorSSTable, cursor.Compress1(), cursor.Compress2(), cursor.OneFile())
			for true {
				fmt.Println("Da li zelite da vidite prethodnu stranu?")
				var yesOrNo string
				_, err = fmt.Scan(&yesOrNo)
				if err != nil {
					panic(err)
				}
				if yesOrNo == "DA" || yesOrNo == "da" || yesOrNo == "Da" {
					iteratorSSTable := scanning.RangeIterateSSTable(rangeVal, compress1, compress2)
					iteratorMem := iterator.NewRangeIterator(cursor, rangeVal)
					strana -= 1
					if strana <= 0 {
						fmt.Println("Dosli ste do prve stranice, nema prethodnih vise.")
						break
					}
					scanning.RANGE_SCAN_OUTPUT(rangeVal, strana, brojNaStrani, iteratorMem, iteratorSSTable, cursor.Compress1(), cursor.Compress2(), cursor.OneFile())

				} else if yesOrNo == "Ne" || yesOrNo == "NE" || yesOrNo == "ne" {
					fmt.Println("Izlazak...")
					break
				} else {
					fmt.Println("Uneta nepostojeca opcija.")
				}
			}
			//pageCache.OutputCurrPage()
			//var nextStopPrev string
			//for {
			//	fmt.Printf(">> ")
			//	_, err := fmt.Scan(&nextStopPrev)
			//	if err != nil {
			//		panic(err)
			//	}
			//	if nextStopPrev == "next" || nextStopPrev == "NEXT" || nextStopPrev == "Next" {
			//		pageCache.IncrementCurrPage()
			//		if pageCache.CheckIfLast() {
			//			scanning.RANGE_SCAN(rangeVal, 1, brojNaStrani, pageCache, iteratorMem, iteratorSSTable, cursor.Compress1(), cursor.Compress2(), cursor.OneFile())
			//
			//		}
			//		fmt.Println("Vasa strana: ")
			//		pageCache.OutputCurrPage()
			//
			//	} else if nextStopPrev == "stop" || nextStopPrev == "STOP" || nextStopPrev == "Stop" {
			//		fmt.Println("Prekidanje...")
			//		break
			//	} else if nextStopPrev == "prev" || nextStopPrev == "PREV" || nextStopPrev == "Prev" {
			//
			//		pageCache.DecrementCurrPage()
			//		pageCache.OutputCurrPage()
			//
			//	} else {
			//		fmt.Println("Pogresna opcija(next, stop ili prev).\n")
			//	}
			//}

		} else if opcijaSken == "3" {
			var prefix string
			var nextStopPrev string
			fmt.Println("Unesite preifx >> ")
			_, err = fmt.Scan(&prefix)
			if err != nil {
				panic(err)
			}

			iteratorCache := iterator.NewIteratingCache(50) //promeniti kasnije

			iteratorSSTable := scanning.PrefixIterateSSTable(prefix, compress1, compress2)
			iteratorMem := iterator.NewPrefixIterator(cursor, prefix)
			for {
				fmt.Printf(">> ")
				_, err := fmt.Scan(&nextStopPrev)
				if err != nil {
					panic(err)
				}
				if nextStopPrev == "next" || nextStopPrev == "NEXT" || nextStopPrev == "Next" {
					iteratorCache.IncrementPosition()
					if iteratorCache.CheckIfLast() {
						data, check := scanning.PREFIX_ITERATE(prefix, iteratorMem, iteratorSSTable, cursor.Compress1(), cursor.Compress2(), cursor.OneFile())

						if check {
							iteratorCache.InsertCache(data)
							fmt.Println("Vas podatak: ")
							fmt.Printf("Kljuc: %s\n\n", data.GetKey())
						} else {
							fmt.Println("Ne posotji elemenata koji zadovoljavaju uslov.")

						}
					} else {
						fmt.Println("Vas podatak: ")
						element := iteratorCache.CurrentElement()
						fmt.Printf("Kljuc: %s\n\n", element.GetKey())

					}

				} else if nextStopPrev == "stop" || nextStopPrev == "STOP" || nextStopPrev == "Stop" {
					fmt.Println("Prekidanje...")
					break
				} else if nextStopPrev == "prev" || nextStopPrev == "PREV" || nextStopPrev == "Prev" {
					if iteratorCache.CurrentPosition() == iteratorCache.MaxNum() {
						iteratorCache.DecrementPosition()
					}
					iteratorCache.DecrementPosition()
					if iteratorCache.CheckIfEnd() {
						fmt.Println("Nema vise elemenata unazad")
						iteratorCache.IncrementPosition()

					} else {
						fmt.Println("Vas podatak: ")
						//iteratorCache.DecrementPosition()
						element := iteratorCache.CurrentElement()
						fmt.Printf("Kljuc: %s\n\n", element.GetKey())

					}
				} else {
					fmt.Println("Pogresna opcija(next, stop ili prev).\n")
				}
			}

		} else if opcijaSken == "4" {
			var rangeVal [2]string
			var nextStopPrev string
			fmt.Println("Unesite odakle >> ")
			_, err = fmt.Scan(&rangeVal[0])
			if err != nil {
				panic(err)
			}
			fmt.Println("Unesite dokle >> ")
			_, err = fmt.Scan(&rangeVal[1])
			if err != nil {
				panic(err)
			}

			iteratorCache := iterator.NewIteratingCache(10) //promeniti kasnije
			iteratorSSTable := scanning.RangeIterateSSTable(rangeVal, compress1, compress2)
			iteratorMem := iterator.NewRangeIterator(cursor, rangeVal)
			for {
				fmt.Printf(">> ")
				_, err := fmt.Scan(&nextStopPrev)
				if err != nil {
					panic(err)
				}
				if nextStopPrev == "next" || nextStopPrev == "NEXT" || nextStopPrev == "Next" {
					iteratorCache.IncrementPosition()
					if iteratorCache.CheckIfLast() {
						data, check := scanning.RANGE_ITERATE(rangeVal, iteratorMem, iteratorSSTable, cursor.Compress1(), cursor.Compress2(), cursor.OneFile())

						if check {
							iteratorCache.InsertCache(data)
							fmt.Println("Vas podatak: ")
							fmt.Printf("Kljuc: %s\n\n", data.GetKey())
						} else {
							fmt.Println("Ne posotji elemenata koji zadovoljavaju uslov.")

						}
					} else {
						fmt.Println("Vas podatak: ")
						element := iteratorCache.CurrentElement()
						fmt.Printf("Kljuc: %s\n\n", element.GetKey())

					}

				} else if nextStopPrev == "stop" || nextStopPrev == "STOP" || nextStopPrev == "Stop" {
					fmt.Println("Prekidanje...")
					break
				} else if nextStopPrev == "prev" || nextStopPrev == "PREV" || nextStopPrev == "Prev" {
					if iteratorCache.CurrentPosition() == iteratorCache.MaxNum() {
						iteratorCache.DecrementPosition()
					}
					iteratorCache.DecrementPosition()
					if iteratorCache.CheckIfEnd() {
						fmt.Println("Nema vise elemenata unazad")
						iteratorCache.IncrementPosition()

					} else {
						fmt.Println("Vas podatak: ")
						//iteratorCache.DecrementPosition()
						element := iteratorCache.CurrentElement()
						fmt.Printf("Kljuc: %s\n\n", element.GetKey())

					}
				} else {
					fmt.Println("Pogresna opcija(next, stop ili prev).\n")
				}

			}
		} else if opcijaSken == "5" {
			fmt.Printf("Izlazak..\n")
			break
		} else {
			fmt.Printf("\nIzabrali ste pogresnu opcjiu.")
		}

	}
}

func Types(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, memtable *cursor.Cursor) {

	for true {
		fmt.Println("\n1. Bloomfilter\n2. Count min sketch\n3. Hyperloglog\n4. Simhash\n5. Izlaz")
		var opcijaTip string
		_, err := fmt.Scan(&opcijaTip)
		if err != nil {
			panic(err)
		}
		if opcijaTip == "1" {
			TypeBloomFilter(wal, lru1, memtable)
		} else if opcijaTip == "2" {
			TypeCountMinSketch(wal, lru1, memtable)
		} else if opcijaTip == "3" {
			TypeHyperLogLog(wal, lru1, memtable)
		} else if opcijaTip == "4" {
			TypeSimHash(wal, lru1, memtable)
		} else if opcijaTip == "5" {
			fmt.Println("\nIzlazak iz opcije tipovi.")
			return
		} else {
			fmt.Println("\nIzabrali ste nepostojecu opciju. Pokusajte ponovo.")
		}
	}
}

func meni(wal *wal_implementation.WriteAheadLog, lru1 *lru.LRUCache, memtable *cursor.Cursor, tokenb *token_bucket.TokenBucket) {
	for true {
		var opcija string
		fmt.Println("\n--------------------------------------------------------\nKey-Value Engine")

		fmt.Println("\n1. Unesi podatak\n2. Obrisi podatak\n3. Dobavi podatak\n4. Skeniranje\n5. Tipovi\n6. Proveri SSTabelu\n7. Izlaz")
		fmt.Printf("Unesite opciju >> ")
		_, err := fmt.Scan(&opcija)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		mess, moze := tokenb.IsRequestAllowed(1)
		if !moze {
			fmt.Printf("\n" + mess + "\n")
			continue
		}

		if opcija == "1" {
			fmt.Printf("Unesite kljuc >> ")
			_, err := fmt.Scan(&key)

			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				continue
			}
			fmt.Printf("Unesite vrednost >> ")
			_, err = fmt.Scan(&value)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			PUT(wal, memtable, key, []byte(value))
		} else if opcija == "2" {
			fmt.Printf("Unesite kljuc >> ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				continue
			}
			DELETE(wal, lru1, memtable, key)
		} else if opcija == "3" {
			fmt.Printf("Unesite kljuc >> ")
			_, err := fmt.Scan(&key)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//provera da li KEY sadrzi neku od kljucnih reci na kraju reci
			ok, kr := checkKey(key)
			if !ok {
				fmt.Printf("Koristite kljucnu rec %s\n", kr)
				continue
			}
			GET(lru1, memtable, key)
		} else if opcija == "4" {
			Scan(memtable)
		} else if opcija == "5" {
			Types(wal, lru1, memtable)
			// TODO test
		} else if opcija == "6" {
			fmt.Println("Unesite koj sstabelu zelite da proverite(npr. sstable1) >> ")
			var sstableName string
			_, err = fmt.Scan(&sstableName)
			if err != nil {
				panic(err)
			}
			_, err = os.Stat(sstableName)
			if err != nil {
				fmt.Println("Ne postoji zadata sstabela.")
			} else {
				ValidateSSTable(sstableName)

			}
			//TEST DONE

		} else if opcija == "7" {
			fmt.Println("\nGasenje programa...")
			break
		} else {
			fmt.Printf("\nIzabrali ste pogresnu opciju. Pokusajte ponovo.")
		}
	}

}

func scantest() {
	var mapMem map[*hashmem.Memtable]int
	//prefix := "1"
	mapMem = make(map[*hashmem.Memtable]int)

	j := 0

	for i := 0; i < 5; i++ {
		btm := hashmem.Memtable(hashstruct.CreateHashMemtable(15))
		for k := 0; k < 14; k++ {
			btm.AddElement(strconv.Itoa(k), []byte(strconv.Itoa(k)), time.Now())

		}
		btm.SendToSSTable(compress1, compress2, oneFile, 2, 3, maxSSTLevel, p)

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
	//iterMem := iterator.NewPrefixIterator(mapMem, prefix)
	//iterSSTable := scanning.PrefixIterateSSTable(prefix, compress2, compress1, oneFile)
	//scanning.PREFIX_SCAN_OUTPUT(prefix, 1, 10, iterMem, iterSSTable, compress1, compress2, oneFile)
	//
	//for k, _ := range mapMem {
	//	mapMem[k] = 0
	//}
	//j = 0
	//valRange := [2]string{"1", "2"}
	//iterMemR := iterator.NewRangeIterator(mapMem, valRange)
	//iterSSTableR := scanning.RangeIterateSSTable(valRange, compress2, compress1, oneFile)
	//scanning.RANGE_SCAN_OUTPUT(valRange, 1, 10, iterMemR, iterSSTableR, compress1, compress2, oneFile)
	fmt.Println("")
}

func skripta(flag bool, wal *wal_implementation.WriteAheadLog, memtable *cursor.Cursor) {
	if flag {
		for i := 1; i <= 2; i++ {
			for j := 1; j <= 50000; j++ {
				PUT(wal, memtable, strconv.Itoa(j), []byte(strconv.Itoa(j)))
			}

		}
	} else {
		for i := 0; i < 1000; i++ {
			for j := 1; j <= 100; j++ {
				PUT(wal, memtable, strconv.Itoa(j), []byte(strconv.Itoa(j)))
			}
		}
	}

}

func main() {
	// postavka
	setConst()
	//kreiranje potrebnih instanci
	wal := wal_implementation.NewWriteAheadLog(walSegmentSize)
	tokenb := token_bucket.NewTokenBucket(rate, maxToken)
	tokenb.InitRequestsFile("token_bucket/requests.bin")
	lru1 := lru.NewLRUCache(lruCap)
	memtable := cursor.NewCursor(memType, memTableNumber, lru1, compress1, compress2, oneFile, N, M, NumberOfSST, memTableCap, compType, maxSSTLevel, levelPlus, p)
	memtable.Fill(wal)

	meni(wal, lru1, memtable, tokenb)

}
