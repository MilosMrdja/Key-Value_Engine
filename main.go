package main

import (
	"KeyValueEngine/SimHash"
	"fmt"
)

func main() {
	sm := SImHash.NewSimHash("i like to play a football")
	fmt.Println("Key Value engine")
	fmt.Println(sm.ReturnIdArray())
}
