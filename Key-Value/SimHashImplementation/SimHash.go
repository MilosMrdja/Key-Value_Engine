package SimHash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/bbalet/stopwords"
	"hash/fnv"
	"strconv"
	"strings"
)

type Tuple struct {
	Weight   uint32 //weight can not be less than zero
	ArrayBit []byte
}

type SimHash struct {
	text        string
	textArray   []string
	fingerprint []int8
	//niz fingerprint
	table map[string]Tuple
	//mapa koja ima kljuc rec i vrednots listu od dva elementa, prvi element je tezina reci a drugi niz jedinica i nula
}

func NewSimHash(t string) *SimHash {

	return &SimHash{
		t,
		make([]string, 0),
		make([]int8, 0),
		make(map[string]Tuple),
	}
}

func (sm *SimHash) RemoveStopWords() []string {

	withoutStopWord := stopwords.CleanString(sm.text, "en", true)

	t := strings.Split(withoutStopWord, " ")
	sm.textArray = t    //does not work :  sm.textArray := strings.Split(sm.text, " ")
	return sm.textArray //return if you want
}

func (sm *SimHash) SetWeightToWords() {

	for i := 0; i < len(sm.textArray); i++ {
		x, ok := sm.table[sm.textArray[i]] //returns Tuple(uint32, array of bits)
		if ok {
			sm.table[sm.textArray[i]] = Tuple{Weight: 1 + x.Weight, ArrayBit: x.ArrayBit}
		} else {
			hash := fnv.New64()
			_, err := hash.Write([]byte(sm.textArray[i]))
			if err != nil {
				return
			}
			resultHash := hash.Sum64()

			sm.table[sm.textArray[i]] = Tuple{Weight: 1, ArrayBit: decimalToBits(resultHash)}
		}
	}

}

func (sm *SimHash) ReturnIdArray() []byte {
	sm.RemoveStopWords()
	sm.SetWeightToWords()
	idArr := make([]int32, 64)
	var bitPosition int8 //bit
	var sum int32

	for i := 0; i < 64; i++ {
		sum = 0
		for _, value := range sm.table {

			bitPosition = int8(value.ArrayBit[i])
			if bitPosition == 0 {
				bitPosition = -1
			}
			sum += int32(value.Weight * uint32(bitPosition))
		}
		idArr[i] = sum

	}

	return makeArray01(idArr)
}

func makeArray01(arr []int32) []byte {
	array := make([]byte, 64)
	for i := 0; i < len(arr); i++ {
		if arr[i] <= 0 {
			array[i] = 0
		} else {
			array[i] = 1
		}
	}
	return array
}

func decimalToBits(num uint64) []byte {
	bits := make([]byte, 64)
	binaryStr := strconv.FormatInt(int64(num), 2) //pretvara 64-bitni broj u string od 64 karaktera

	temp := len(binaryStr) - 1
	tempb := 63
	for i := temp; i >= 0; i-- {
		if binaryStr[i] == '1' {
			bits[tempb] = 1
		} else {
			bits[tempb] = 0
		}
		tempb--
	}
	if binaryStr[0] == '-' {
		bits[0] = 1
	} else {
		bits[0] = 0
	}
	return bits
}
func HammingDistance(arr1, arr2 []byte) int8 {
	result := 0
	for i := 0; i < len(arr1); i++ {
		if arr1[i] != arr2[i] {
			result += 1
		}
	}
	return int8(result)
}

func (sm *SimHash) SerializeSimHash() ([]byte, error) {
	buffer := new(bytes.Buffer)

	// Write text, textArray, and fingerprint to the buffer
	binary.Write(buffer, binary.LittleEndian, int32(len(sm.text)))
	binary.Write(buffer, binary.LittleEndian, []byte(sm.text))

	binary.Write(buffer, binary.LittleEndian, int32(len(sm.textArray)))
	for _, str := range sm.textArray {
		binary.Write(buffer, binary.LittleEndian, int32(len(str)))
		binary.Write(buffer, binary.LittleEndian, []byte(str))
	}

	binary.Write(buffer, binary.LittleEndian, int32(len(sm.fingerprint)))
	for _, val := range sm.fingerprint {
		binary.Write(buffer, binary.LittleEndian, int8(val))
	}

	return buffer.Bytes(), nil
}

func DeserializeSimHash(data []byte) (*SimHash, error) {
	buffer := bytes.NewReader(data)

	var textLen int32
	err := binary.Read(buffer, binary.LittleEndian, &textLen)
	if err != nil {
		return nil, err
	}

	textBytes := make([]byte, textLen)
	err = binary.Read(buffer, binary.LittleEndian, textBytes)
	if err != nil {
		return nil, err
	}
	text := string(textBytes)

	var textArrayLen int32
	err = binary.Read(buffer, binary.LittleEndian, &textArrayLen)
	if err != nil {
		return nil, err
	}

	textArray := make([]string, textArrayLen)
	for i := 0; i < int(textArrayLen); i++ {
		var strLen int32
		err = binary.Read(buffer, binary.LittleEndian, &strLen)
		if err != nil {
			return nil, err
		}

		strBytes := make([]byte, strLen)
		err = binary.Read(buffer, binary.LittleEndian, strBytes)
		if err != nil {
			return nil, err
		}
		textArray[i] = string(strBytes)
	}

	var fingerprintLen int32
	err = binary.Read(buffer, binary.LittleEndian, &fingerprintLen)
	if err != nil {
		return nil, err
	}

	fingerprint := make([]int8, fingerprintLen)
	for i := 0; i < int(fingerprintLen); i++ {
		var val int8
		err = binary.Read(buffer, binary.LittleEndian, &val)
		if err != nil {
			return nil, err
		}
		fingerprint[i] = val
	}

	mapa := make(map[string]Tuple)

	return &SimHash{text: text, textArray: textArray, fingerprint: fingerprint, table: mapa}, nil
}

func main() {

	text := "This is an example text for testing SimHash."
	simHashInstance := NewSimHash(text)
	idArray := simHashInstance.ReturnIdArray()

	fmt.Println("Original Text:", text)
	fmt.Println("SimHash ID Array:", idArray)

	serializedData, err := simHashInstance.SerializeSimHash()
	if err != nil {
		fmt.Println("Serialization error:", err)
		return
	}

	fmt.Println("\nSerialized data:", string(serializedData))

	deserializedSimHash, err := DeserializeSimHash(serializedData)
	if err != nil {
		fmt.Println("Deserialization error:", err)
		return
	}

	fmt.Println("\nDeserialized Text:", deserializedSimHash.text)
	fmt.Println("Deserialized SimHash ID Array:", deserializedSimHash.ReturnIdArray())
}
