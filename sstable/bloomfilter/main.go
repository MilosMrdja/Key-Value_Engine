package main

import (
	"bloom/bloomfilter"
	"fmt"
)

func main() {
	bitsetSize := 10
	bloom := bloomfilter.CreateBloomFilter(bitsetSize)
	a := bloom.Get([]byte("Nikola"))
	if a == false {
		bloom.Set([]byte("Nikola"))
	}
	a = bloom.Get([]byte("Nikola"))
	err := bloomfilter.SerializeBloomFilter(bloom, "bloomfilter.bin")
	if err != nil {
		panic(err)
	}
	bf, err := bloomfilter.DeserializeBloomFilter("bloomfilter.bin")
	if err != nil {
		panic(err)
	}
	fmt.Println(bf.Get([]byte("Nikola")))
}
