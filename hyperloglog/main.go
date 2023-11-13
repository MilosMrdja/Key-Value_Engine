package main

import (
	"encoding/binary"
	"fmt"
	"hyperloglog/hyperloglog"
)

func main() {

	h := hyperloglog.CreateHyperLogLog(65536)
	dis := make([]uint32, 100000000)
	for i := 0; i < 100000000; i++ {
		dis[i] = uint32(i + 1)
	}
	cd := hyperloglog.ClassicCountDistinct(dis)
	fmt.Println("classic: %v\n", cd)
	for i := 0; i < 100000000; i++ {
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, uint32(i))

		h.Add(bs)
	}
	hd := h.CountHLL()
	fmt.Printf("hyperloglog: %v\n", hd)
	err := hyperloglog.SerializeHyperLogLog(h, "hyperloglog.bin")
	if err != nil {
		return
	}
	log, err := hyperloglog.DeserializeHyperLogLog("hyperloglog.bin")
	if err != nil {
		panic(err)
	}
	hd = log.CountHLL()
	fmt.Printf("Serialized: %v\n", hd)

}
