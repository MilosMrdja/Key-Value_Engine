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

func leviAktivniBit(x uint64) int64 {

	return int64(1 + bits.LeadingZeros64(x))
}

type HyperLogLog struct {
	registers []int64
	m         uint64
	b         uint64
	alpha     float64
}

func SaveHyperLogLogToFile(hll *HyperLogLog, fileName string) error {

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

	registerArray := make([]byte, 8)
	var i uint64 = 0
	for i = 0; i < hll.m; i++ {
		binary.BigEndian.PutUint64(registerArray, uint64(hll.registers[i]))
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

func SerializeHyperLogLog(hll *HyperLogLog) ([]byte, error) {

	resultArray := make([]byte, 0)
	numOfBuckets := make([]byte, 8)
	binary.BigEndian.PutUint64(numOfBuckets, hll.b)
	resultArray = append(resultArray, numOfBuckets...)

	alphaArray := make([]byte, 8)
	binary.BigEndian.PutUint64(alphaArray[:], math.Float64bits(hll.alpha))
	resultArray = append(resultArray, alphaArray...)

	registerNumArray := make([]byte, 8)
	binary.BigEndian.PutUint64(registerNumArray, hll.m)
	resultArray = append(resultArray, registerNumArray...)

	registerArray := make([]byte, 8)
	var i uint64 = 0
	for i = 0; i < hll.m; i++ {
		binary.BigEndian.PutUint64(registerArray, uint64(hll.registers[i]))
		resultArray = append(resultArray, registerArray...)

	}
	return resultArray, nil
}

func DeserializeHyperLogLog(byteArray []byte) (*HyperLogLog, error) {

	curr_offset := 8
	b := byteArray[:curr_offset]
	numBuckets := binary.BigEndian.Uint64(b)

	b = byteArray[curr_offset : curr_offset+8]
	curr_offset += 8
	alphaVal := math.Float64frombits(binary.BigEndian.Uint64(b))

	b = byteArray[curr_offset : curr_offset+8]
	curr_offset += 8
	numRegisters := binary.BigEndian.Uint64(b)

	var i uint64 = 0
	register := make([]int64, numRegisters)
	for i = 0; i < numRegisters; i++ {

		registerArray := byteArray[curr_offset : curr_offset+8]
		curr_offset += 8
		register[i] = int64(binary.BigEndian.Uint64(registerArray))
	}
	hll := HyperLogLog{
		registers: register, m: numRegisters, b: numBuckets, alpha: alphaVal,
	}
	return &hll, nil
}

func LoadHyperLogLogFromFile(fileName string) (*HyperLogLog, error) {
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

	registerArray := make([]byte, 8)
	var i uint64 = 0
	register := make([]int64, numRegisters)
	for i = 0; i < numRegisters; i++ {
		err := binary.Read(file, binary.BigEndian, registerArray)
		if err != nil {
			return nil, err
		}
		register[i] = int64(binary.BigEndian.Uint64(registerArray))
	}
	hll := HyperLogLog{
		registers: register, m: numRegisters, b: numBuckets, alpha: alphaVal,
	}
	return &hll, nil
}

func NextPowerOfTwo(num uint64) uint64 {
	x := 1
	for true {
		x = x << 1
		if uint64(x) > num {
			break
		}
	}
	return uint64(x)
}

func CreateHyperLogLog(m uint64) *HyperLogLog {
	m = NextPowerOfTwo(m)
	return &HyperLogLog{
		registers: make([]int64, m),
		m:         m,
		b:         uint64(math.Ceil(math.Log2(float64(m)))),
		alpha:     0.7213 / (1 + (1.079 / math.Pow(2.0, math.Ceil(math.Log2(float64(m))))))}
}

func (hyperloglog *HyperLogLog) Add(input []byte) {
	hesh := hash64(input)
	prvihBBita := 64 - hyperloglog.b
	result := leviAktivniBit(hesh)
	j := (hesh >> prvihBBita)

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
		i := rand.Uint64()
		bajts := make([]byte, 8)
		binary.LittleEndian.PutUint64(bajts, i)
		izlaz = append(izlaz, bajts)
	}
	return
}
func ClassicCountDistinct(input []uint64) int {
	m := map[uint64]struct{}{}
	for _, i := range input {
		if _, ok := m[i]; !ok {
			m[i] = struct{}{}
		}
	}
	return len(m)
}
func hash64(input []byte) uint64 {

	hashFunc := murmur3.New64()
	_, err := hashFunc.Write(input)
	if err != nil {
		panic(err)
	}
	suma := hashFunc.Sum64()
	hashFunc.Reset()
	return suma
}
