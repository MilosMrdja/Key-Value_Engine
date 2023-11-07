package SimHash

import (
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
	fmt.Println(withoutStopWord)
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

			fmt.Println(resultHash)
			sm.table[sm.textArray[i]] = Tuple{Weight: 1, ArrayBit: decimalToBits(resultHash)}
		}
	}

}

func (sm *SimHash) ReturnIdArray() []int8 {
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

func makeArray01(arr []int32) []int8 {
	array := make([]int8, 64)
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
	fmt.Println(binaryStr)
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
