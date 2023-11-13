package hyperloglog

import (
	"encoding/binary"
	"github.com/twmb/murmur3"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"time"
)

func leviAktivniBit(x uint32) int32 {

	return int32(1 + bits.TrailingZeros32(x))
}

type HyperLogLog struct {
	registers []int32
	m         uint64
	b         uint64
	alpha     float64
}

func SerializeHyperLogLog(hll *HyperLogLog, fileName string) error {

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

	numOfBuckets := make([]byte, 8)
	binary.BigEndian.PutUint64(numOfBuckets, hll.b)
	_, err = file.Write(numOfBuckets)
	if err != nil {
		return err
	}

	alphaArray := make([]byte, 8)
	binary.BigEndian.PutUint64(alphaArray[:], math.Float64bits(hll.alpha))
	_, err = file.Write(alphaArray)
	if err != nil {
		return err
	}

	registerNumArray := make([]byte, 8)
	binary.BigEndian.PutUint64(registerNumArray, hll.m)
	_, err = file.Write(registerNumArray)
	if err != nil {
		return err
	}

	registerArray := make([]byte, 4)
	var i uint64 = 0
	for i = 0; i < hll.m; i++ {
		binary.BigEndian.PutUint32(registerArray, uint32(hll.registers[i]))
		_, err = file.Write(registerArray)
		if err != nil {
			return err
		}

	}

	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func DeserializeHyperLogLog(fileName string) (*HyperLogLog, error) {
	_, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(0, 0)

	if err != nil {
		return nil, err
	}

	b := make([]byte, 8)
	err = binary.Read(file, binary.BigEndian, b)
	if err != nil {
		return nil, err
	}
	numBuckets := binary.BigEndian.Uint64(b)

	b = make([]byte, 8)
	err = binary.Read(file, binary.BigEndian, b)
	if err != nil {
		return nil, err
	}
	alphaVal := math.Float64frombits(binary.BigEndian.Uint64(b))
	b = make([]byte, 8)
	err = binary.Read(file, binary.BigEndian, b)
	if err != nil {
		return nil, err
	}
	numRegisters := binary.BigEndian.Uint64(b)

	registerArray := make([]byte, 4)
	var i uint64 = 0
	register := make([]int32, numRegisters)
	for i = 0; i < numRegisters; i++ {
		err := binary.Read(file, binary.BigEndian, registerArray)
		if err != nil {
			return nil, err
		}
		register[i] = int32(binary.BigEndian.Uint32(registerArray))
	}
	hll := HyperLogLog{
		registers: register, m: numRegisters, b: numBuckets, alpha: alphaVal,
	}
	return &hll, nil
}

func CreateHyperLogLog(m uint64) *HyperLogLog {
	return &HyperLogLog{
		registers: make([]int32, m),
		m:         m,
		b:         uint64(math.Ceil(math.Log2(float64(m)))),
		alpha:     0.7213 / (1 + (1.079 / math.Pow(2.0, math.Ceil(math.Log2(float64(m))))))}
}

func (hyperloglog *HyperLogLog) Add(input []byte) {
	hesh := hash32(input)
	prvihBBita := 32 - hyperloglog.b
	result := leviAktivniBit(hesh)
	j := hesh >> prvihBBita

	if result > hyperloglog.registers[j] {
		hyperloglog.registers[j] = result
	}
}
func (hyperloglog *HyperLogLog) CountHLL() uint64 {
	suma := 0.0
	brojElemenata := float64(hyperloglog.m)
	for i := 0; i < int(hyperloglog.m); i++ {
		suma += math.Pow(0.5, float64(hyperloglog.registers[i]))
	}
	procena := hyperloglog.alpha * brojElemenata * brojElemenata / suma
	return uint64(procena)
}
func getRandom() (izlaz [][]byte) { //Funkicija za testiranje
	for i := 0; i < math.MaxInt16; i++ {
		rand.Seed(time.Now().UnixNano())
		i := rand.Uint32()
		bajts := make([]byte, 4)
		binary.LittleEndian.PutUint32(bajts, i)
		izlaz = append(izlaz, bajts)
	}
	return
}
func ClassicCountDistinct(input []uint32) int {
	m := map[uint32]struct{}{}
	for _, i := range input {
		if _, ok := m[i]; !ok {
			m[i] = struct{}{}
		}
	}
	return len(m)
}
func hash32(input []byte) uint32 {

	hashFunc := murmur3.New32()
	_, err := hashFunc.Write(input)
	if err != nil {
		panic(err)
	}
	suma := hashFunc.Sum32()
	hashFunc.Reset()
	return suma
}
