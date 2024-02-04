package bloomfilter

import (
	"encoding/binary"
	"github.com/twmb/murmur3"
	"hash"
	"math"
	"os"
	//"github.com/twmb/murmur3"
)

func generateOptimalNumberOfHashFunctions(m uint64, n uint64) uint64 { //Racuna optimalan broj hash funkcija po formuli
	a := math.Ln2
	k := float64(int64(m)/int64(n)) * a

	return uint64(math.Round(k))
}

func generateHashFunctions(m uint64, n uint64) []hash.Hash32 { //Generise niz hash funkcija pomocu murmur3 biblioteke i seed-a koji je indeks iz niza hashfunkcija
	length := generateOptimalNumberOfHashFunctions(m, n)
	var hashArray []hash.Hash32
	var i uint64 = 0
	for i = 0; i < length; i++ {

		function := murmur3.SeedNew32(uint32(i))
		hashArray = append(hashArray, function)
	}
	return hashArray
}

func calculateProbability(p float64) float64 { //Ovo navodno navodi korisnik

	return (p) * math.Log2(float64(2))
}

func calculateBitsetSize(n uint64, probability float64) uint64 { //Racuna duzinu bitseta u zavisnosi od kolicine elemenata po formuli

	return uint64(-((float64(n) * math.Log(probability)) / math.Pow(math.Ln2, 2)))
}

type BloomFilter struct {
	elemNum       uint64
	bitsetLength  uint64
	bitset        []byte
	numOfHashes   uint64
	hashFunctions []hash.Hash32
	probability   float64
}

func ReadFromFile(file *os.File) (*BloomFilter, error) {
	b := make([]byte, 8)
	err := binary.Read(file, binary.BigEndian, b)
	numElem := binary.BigEndian.Uint64(b)

	b = make([]byte, 8)
	err = binary.Read(file, binary.BigEndian, b)
	if err != nil {
		return nil, err
	}
	prob := binary.BigEndian.Uint64(b)
	probF := math.Float64frombits(prob)

	b = make([]byte, 8)

	err = binary.Read(file, binary.BigEndian, b)
	if err != nil {
		return nil, err
	}
	bitsizeLen := binary.BigEndian.Uint64(b)

	bitsetArray := make([]byte, bitsizeLen)
	err = binary.Read(file, binary.BigEndian, bitsetArray)

	b = make([]byte, 8)
	err = binary.Read(file, binary.BigEndian, b)
	if err != nil {
		return nil, err
	}
	numOfHash := binary.BigEndian.Uint64(b)

	bf := BloomFilter{
		elemNum: numElem, bitsetLength: bitsizeLen, bitset: bitsetArray, numOfHashes: numOfHash, hashFunctions: generateHashFunctions(bitsizeLen, numElem), probability: probF,
	}

	return &bf, nil
}
func DeserializeBloomFilter(bytearray []byte) (*BloomFilter, error) {

	curr_offset := 8
	b := bytearray[:curr_offset]
	numElements := binary.BigEndian.Uint64(b)
	b = bytearray[curr_offset : curr_offset+8]
	prob := binary.BigEndian.Uint64(b)
	probF := math.Float64frombits(prob)
	curr_offset += 8
	b = bytearray[curr_offset : curr_offset+8]

	bitsizeLen := binary.BigEndian.Uint64(b)
	curr_offset += 8

	bitsetArray := make([]byte, bitsizeLen)
	bitsetArray = bytearray[curr_offset : curr_offset+int(bitsizeLen)]
	curr_offset += int(bitsizeLen)

	b = bytearray[curr_offset : curr_offset+8]
	numOfHash := binary.BigEndian.Uint64(b)

	bf := BloomFilter{
		elemNum: numElements, bitsetLength: bitsizeLen, bitset: bitsetArray, numOfHashes: numOfHash, hashFunctions: generateHashFunctions(bitsizeLen, numElements), probability: probF,
	}

	return &bf, nil
}

func SaveToFile(f *BloomFilter, fileName string) error {
	_, err := os.Stat(fileName)
	if err == nil {

		err1 := os.Remove(fileName)
		if err1 != nil {
			return err1
		}
	}

	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}
	elemNumArray := make([]byte, 8)
	binary.BigEndian.PutUint64(elemNumArray, f.elemNum)
	_, err = file.Write(elemNumArray)
	if err != nil {
		return err
	}

	probabilityArray := make([]byte, 8)
	binary.BigEndian.PutUint64(probabilityArray[:], math.Float64bits(f.probability))
	_, err = file.Write(probabilityArray)
	if err != nil {
		return err
	}

	bitsetLengthArray := make([]byte, 8)
	binary.BigEndian.PutUint64(bitsetLengthArray, uint64(f.bitsetLength))

	_, err = file.Write(bitsetLengthArray)
	if err != nil {
		return err
	}

	bitsetArray := f.bitset
	err = binary.Write(file, binary.BigEndian, bitsetArray)
	if err != nil {
		return err
	}

	numOfHashArray := make([]byte, 8)
	binary.BigEndian.PutUint64(numOfHashArray, uint64(f.numOfHashes))
	_, err = file.Write(numOfHashArray)
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func SerializeBloomFilter(f *BloomFilter) ([]byte, error) {

	resultArray := make([]byte, 0)

	elemNumArray := make([]byte, 8)
	binary.BigEndian.PutUint64(elemNumArray, f.elemNum)
	resultArray = append(resultArray, elemNumArray...)

	probabilityArray := make([]byte, 8)
	binary.BigEndian.PutUint64(probabilityArray[:], math.Float64bits(f.probability))

	resultArray = append(resultArray, probabilityArray...)

	bitsetLengthArray := make([]byte, 8)
	binary.BigEndian.PutUint64(bitsetLengthArray, uint64(f.bitsetLength))

	resultArray = append(resultArray, bitsetLengthArray...)

	bitsetArray := f.bitset
	resultArray = append(resultArray, bitsetArray...)

	numOfHashArray := make([]byte, 8)
	binary.BigEndian.PutUint64(numOfHashArray, uint64(f.numOfHashes))

	resultArray = append(resultArray, numOfHashArray...)

	return resultArray, nil
}

func CreateBloomFilter(n uint64, p float64) *BloomFilter { //Constructor

	bloomfilter := BloomFilter{bitsetLength: calculateBitsetSize(n, calculateProbability(p)),
		elemNum:       uint64(n),
		bitset:        make([]byte, calculateBitsetSize(n, calculateProbability(p)), calculateBitsetSize(n, calculateProbability(p))),
		numOfHashes:   generateOptimalNumberOfHashFunctions(calculateBitsetSize(n, calculateProbability(p)), n),
		hashFunctions: generateHashFunctions(calculateBitsetSize(n, calculateProbability(p)), n),
		probability:   calculateProbability(p)}

	return &bloomfilter
}
func (f *BloomFilter) createHash(input []byte) []uint32 {

	var rez []uint32
	var i uint64 = 0
	for i = 0; i < f.numOfHashes; i++ {
		_, err := f.hashFunctions[i].Write(input)
		if err != nil {
			panic(err)
		}
		rez = append(rez, f.hashFunctions[i].Sum32())
		f.hashFunctions[i].Reset()
	}
	return rez
}

func (f *BloomFilter) hashPosition(input []byte) []uint { //Funkcija nalazi pozicije koje su rezultat inputa koji je provucen kroz sve hash funkcije

	var pozicije []uint

	hashes := f.createHash(input)

	for i := 0; i < len(hashes); i++ {
		pozicije = append(pozicije, uint(hashes[i])%uint(len(f.bitset)))
	}
	return pozicije
}
func (f *BloomFilter) Set(s []byte) { //Pise bitove na pozicije
	pos := f.hashPosition(s)
	for i := 0; i < len(pos); i++ {
		f.bitset[pos[i]] = 1
	}

}
func (f *BloomFilter) Get(s []byte) bool { // Proverava da li na osnovu dobijenih pozicija podatak postoji

	hesovane := f.hashPosition(s)
	for i := 0; i < len(hesovane); i++ {
		if f.bitset[hesovane[i]] == 0 {
			return false
		}
	}
	return true
}
