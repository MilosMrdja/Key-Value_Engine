package main

import (
	"SimHashImplementation/SimHash"
	"fmt"
)

func main() {
	fmt.Println("---------------- SimHash ----------------")
	sm := SimHash.NewSimHash("my name is milos mrdja")
	sm2 := SimHash.NewSimHash("i am the biggest world mega king")
	sm3 := SimHash.NewSimHash("i like to move it")
	sm4 := SimHash.NewSimHash("my name is XXX")

	fmt.Println(sm.ReturnIdArray())

	fmt.Println(sm2.ReturnIdArray())

	fmt.Println(sm3.ReturnIdArray())

	fmt.Println(sm4.ReturnIdArray())

	fmt.Println(SimHash.HammingDistance(sm.ReturnIdArray(), sm2.ReturnIdArray()))
	fmt.Println(SimHash.HammingDistance(sm.ReturnIdArray(), sm3.ReturnIdArray()))
	fmt.Println(SimHash.HammingDistance(sm.ReturnIdArray(), sm4.ReturnIdArray()))

}
