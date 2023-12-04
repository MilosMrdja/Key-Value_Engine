package main

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
func main() {
	//t := btree.NewBTree(4) // A B-Tree with minimum degree 3

	//var testS string = ""
	//randa := rand.Intn(10000-1) + 1
	//for i := 1; i <= 10000; i++ {
	//	if i == randa {
	//		testS = RandStringRunes(10)
	//		t.Insert(testS)
	//	} else {
	//		t.Insert(RandStringRunes(10))
	//
	//	}
	//}
	//
	//output := t.Search(testS)
	//fmt.Println(output)

}
