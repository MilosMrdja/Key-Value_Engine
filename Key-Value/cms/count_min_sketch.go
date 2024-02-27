package count_min_sketch

import (
	"bytes"
	"encoding/binary"
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

// SerializeCountMinSketch serializes the CMS to a byte slice
func (cms *CountMinSketch) SerializeCountMinSketch() ([]byte, error) {
	buffer := new(bytes.Buffer)

	// Write width and hashes to the buffer
	binary.Write(buffer, binary.LittleEndian, int32(cms.width))
	binary.Write(buffer, binary.LittleEndian, int32(cms.hashes))

	// Write the table data to the buffer
	for _, row := range cms.table {
		for _, value := range row {
			binary.Write(buffer, binary.LittleEndian, int32(value))
		}
	}

	return buffer.Bytes(), nil
}

// DeserializeCountMinSketch deserializes the CMS from a byte slice
func DeserializeCountMinSketch(data []byte) (*CountMinSketch, error) {
	buffer := bytes.NewReader(data)

	var width, hashes int32

	// Read width and hashes from the buffer
	err := binary.Read(buffer, binary.LittleEndian, &width)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buffer, binary.LittleEndian, &hashes)
	if err != nil {
		return nil, err
	}

	// Read the table data from the buffer
	table := make([][]int, int(hashes))
	for i := range table {
		table[i] = make([]int, int(width))
		for j := range table[i] {
			var value int32
			err := binary.Read(buffer, binary.LittleEndian, &value)
			if err != nil {
				return nil, err
			}
			table[i][j] = int(value)
		}
	}

	return &CountMinSketch{width: int(width), hashes: int(hashes), table: table}, nil
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
