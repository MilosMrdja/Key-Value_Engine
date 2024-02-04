package SSTable

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sstable/MerkleTreeImplementation/MerkleTree"
	"sstable/bloomfilter/bloomfilter"
	"sstable/mem/memtable/datatype"
)

type SSTable struct {
	bloomFilter *bloomfilter.BloomFilter //referenca?
	merkleTree  *MerkleTree.MerkleTree
	summary     map[string]int
	index       map[string]int
	data        []byte
}

// N i M su nam redom razudjenost u index-u, i u summary-ju
func NewSSTable(dataList []datatype.DataType, proability_bf float64, N, M int, fileName string, compress1, compress2, oneFile bool) bool {

	// pomocne promenljive
	arrToMerkle := make([][]byte, 0)
	var serializedData, indexData []byte
	var duzinaPodatka, acc, accIndex, duzinaDataList int
	acc = 0
	accIndex = 0
	duzinaDataList = len(dataList)
	var err error
	bloomFilter := bloomfilter.CreateBloomFilter(uint64(duzinaDataList), proability_bf)

	// mapa za enkodirane vrednosti
	dictionary, err := DeserializationHashMap("EncodedKeys.bin")
	if err != nil {
		panic(err)
	}
	//fmt.Printf("Mapa sa starim kljucevima\n")
	//for k, v := range *dictionary {
	//	fmt.Printf("\nMAPA[%s] -> %d\n", k, v)
	//}

	//Data fajl
	file, err := os.OpenFile(fileName+"/Data.bin", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return false
	}
	defer file.Close()

	//Index fajl
	fileIndex, err2 := os.OpenFile(fileName+"/Index.bin", os.O_WRONLY|os.O_CREATE, 0666)
	if err2 != nil {
		return false
	}
	defer fileIndex.Close()

	//Symmary fajl
	fileSummary, err3 := os.OpenFile(fileName+"/Summary.bin", os.O_WRONLY|os.O_CREATE, 0666)
	if err3 != nil {
		return false
	}
	defer fileSummary.Close()

	// upis prvog i poslednjeg

	minData, err := SerializeIndexData(dataList[0].GetKey(), accIndex, compress1, compress2, (*dictionary)[dataList[0].GetKey()])
	if err != nil {
		return false
	}
	fileSummary.Write(minData)

	maxData, err := SerializeIndexData(dataList[duzinaDataList-1].GetKey(), accIndex, compress1, compress2, (*dictionary)[dataList[duzinaDataList-1].GetKey()])
	if err != nil {
		return false
	}
	fileSummary.Write(maxData)
	// glavna petlja

	for i := 0; i < duzinaDataList; i++ {
		// u mapu dodamo encodiranu vrednost kljuca

		_, exist := (*dictionary)[dataList[i].GetKey()]
		if !exist {
			(*dictionary)[dataList[i].GetKey()] = int32(len(*(dictionary)) + 1)
		}
		// dodali smo kljuc u bloomf
		AddKeyToBloomFilter(bloomFilter, dataList[i].GetKey())

		// serijaliacija podatka
		serializedData, err = SerializeDataType(dataList[i], compress1, compress2, (*dictionary)[dataList[i].GetKey()])
		if err != nil {
			return false
		}

		// upisujemo podatak u Data.bin fajl
		duzinaPodatka, err = file.Write(serializedData)
		if err != nil {
			return false
		}

		//Upis odgovarajucih vrednosti u Summary
		if (i+1)%M == 0 {
			indexData, err = SerializeIndexData(dataList[i].GetKey(), accIndex, compress1, compress2, (*dictionary)[dataList[i].GetKey()])
			if err != nil {
				return false
			}
			fileSummary.Write(indexData)
		}
		//Upis odgovarajucih vrednosti u Index
		if (i+1)%N == 0 {
			indexData, err = SerializeIndexData(dataList[i].GetKey(), acc, compress1, compress2, (*dictionary)[dataList[i].GetKey()])
			if err != nil {
				return false
			}
			fileIndex.Write(indexData)
			accIndex += len(indexData)

		}

		acc += duzinaPodatka

		// pomocni niz koji presludjemo za MerkleTree
		arrToMerkle = append(arrToMerkle, serializedData)

	}
	//Kreiranje i upis Merkle Stabla
	CreateMerkleTree(arrToMerkle, fileName+"/Merkle.bin")

	//Serijalizacija i upis bloom filtera
	err = bloomfilter.SaveToFile(bloomFilter, fileName+"/BloomFilter.bin")
	if err != nil {
		return false
	}
	//fmt.Printf("%d\n", acc)

	// u slucaju da korisnik odabere sve u jedan fajl
	var serializedInOneFile []byte
	if oneFile {
		serializedInOneFile, err = WriteToOneFile(fileName+"/BloomFilter.bin", fileName+"/Summary.bin", fileName+"/Index.bin", fileName+"/Data.bin", fileName+"/Merkle.bin")
		if err != nil {
			panic(err)
		}

		// One file
		file.Close()
		fileNameOneFile := fileName + "/SSTable.bin"
		e := os.Rename(fileName+"/Data.bin", fileNameOneFile)
		if e != nil {
			log.Fatal(e)
		}
		fileOne, err2 := os.OpenFile(fileNameOneFile, os.O_WRONLY|os.O_CREATE, 0666)
		if err2 != nil {
			panic(err2)
		}
		fileOne.Seek(0, 2)
		fileOne.Truncate(int64(len(serializedInOneFile)))
		fileOne.Write(serializedInOneFile)
		fileOne.Close()

		fileInfo, err := os.Stat(fileNameOneFile)
		if err != nil {
			panic(err)
		}
		end := fileInfo.Size()

		fmt.Printf("Velicina SST: %d\n", end)

		fileSummary.Close()
		fileIndex.Close()
		err = os.Remove(fileName + "/BloomFilter.bin")
		err = os.Remove(fileName + "/Summary.bin")
		err = os.Remove(fileName + "/Index.bin")
		err = os.Remove(fileName + "/Merkle.bin")

	}
	_, err = SerializeHashmap("EncodedKeys.bin", dictionary)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("Mapa sa novim kljucevima")
	//for k, v := range *dictionary {
	//	fmt.Printf("\nMAPA[%s] -> %d\n", k, v)
	//}

	return true
}

// f-ja koja kreira merkle stablo, vraca True ako je uspesno kreirano, u suprotnom False
func CreateMerkleTree(data [][]byte, fileName string) bool {
	merkleTree, err := MerkleTree.CreateMerkleTree(data)

	if err != nil {
		return false
	}
	_, err3 := MerkleTree.SerializeMerkleTree(merkleTree, fileName)
	if err3 != nil {
		return false
	}
	return true
}

// f-ja koja dodaje kljuc u bloomfilter i vraca True ako je uspesno dodao
func AddKeyToBloomFilter(bloomFilter *bloomfilter.BloomFilter, key string) bool {
	bloomFilter.Set([]byte(key))
	return true
}

/*
position 1 === bloom
position 2 === summary
position 3 === index
position 4 === merkled
position 5 === data
*/
func PositionInSSTable(file os.File, position int) (int64, int64) {
	var bytes []byte
	var size, sizeEnd int64

	//procita pocetak segmenta
	if position == 5 {
		size = 0
	} else {
		file.Seek(int64(-4*(position+1)), 2)
		bytes = make([]byte, 4)
		_, err := file.Read(bytes)
		if err != nil {
			panic(err)
		}
		size = int64(binary.BigEndian.Uint32(bytes))
	}

	//cita kraj segmenta
	file.Seek(int64(-4*position), 2)
	bytes = make([]byte, 4)
	_, err := file.Read(bytes)
	if err != nil {
		panic(err)
	}
	sizeEnd = int64(binary.BigEndian.Uint32(bytes))

	return size, sizeEnd
}
