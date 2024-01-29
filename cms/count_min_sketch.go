package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
)

// CountMinSketch struktura
type CountMinSketch struct {
	width  int
	hashes int
	table  [][]int
}

// NewCountMinSketch inicijalizacija CountMinSketch-a sa brojem kolona i hes funkcija
func NewCountMinSketch(width, hashes int) *CountMinSketch {
	table := make([][]int, hashes)
	for i := range table {
		table[i] = make([]int, width)
	}
	return &CountMinSketch{width: width, hashes: hashes, table: table}
}

// Update funkcija povecava vrednost u hes tabeli za 1
func (cms *CountMinSketch) Update(key string) {
	for i := 0; i < cms.hashes; i++ {
		hashValue := hash(key, i) % uint32(cms.width)
		cms.table[i][hashValue]++
	}
}

// Estimate vraca broj pojavljivanja vrednosti u tabeli CMS-a
func (cms *CountMinSketch) Estimate(key string) int {
	minCount := cms.table[0][hash(key, 0)%uint32(cms.width)]
	for i := 1; i < cms.hashes; i++ {
		count := cms.table[i][hash(key, i)%uint32(cms.width)]
		if count < minCount {
			minCount = count
		}
	}
	return minCount
}

// Destroy brise CMS objekat
func (cms *CountMinSketch) Destroy() {

	for i := range cms.table {
		cms.table[i] = nil
	}
	cms.table = nil
}

// hash generise hes vrednost
func hash(s string, index int) uint32 {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0
	}
	return (h.Sum32() + uint32(index)) % maxUint32
}

// SerializeCountMinSketch serijalizuje CMS
func (cms *CountMinSketch) SerializeCountMinSketch() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(struct {
		Width  int
		Hashes int
		Table  [][]int
	}{cms.width, cms.hashes, cms.table})

	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// DeserializeCountMinSketch deserijalizuje CMS
func DeserializeCountMinSketch(data []byte) (*CountMinSketch, error) {
	var cms CountMinSketch
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	temp := struct {
		Width  int
		Hashes int
		Table  [][]int
	}{}

	err := decoder.Decode(&temp)
	if err != nil {
		return nil, err
	}

	cms.width = temp.Width
	cms.hashes = temp.Hashes
	cms.table = temp.Table

	return &cms, nil
}

const maxUint32 = ^uint32(0)

func main() {
	width := 100
	hashes := 5
	cms := NewCountMinSketch(width, hashes)

	cms.Update("apple")
	cms.Update("banana")
	cms.Update("apple")
	cms.Update("orange")

	serializedData, err := cms.SerializeCountMinSketch()
	if err != nil {
		fmt.Println("Error serializing CountMinSketch:", err)
		return
	}

	deserializedCMS, err := DeserializeCountMinSketch(serializedData)
	if err != nil {
		fmt.Println("Error deserializing CountMinSketch:", err)
		return
	}

	fmt.Printf("Count of 'apple': %d\n", deserializedCMS.Estimate("apple"))
	fmt.Printf("Count of 'banana': %d\n", deserializedCMS.Estimate("banana"))
	fmt.Printf("Count of 'orange': %d\n", deserializedCMS.Estimate("orange"))
	fmt.Printf("Count of 'grape': %d\n", deserializedCMS.Estimate("grape"))

}
