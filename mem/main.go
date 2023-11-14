package main

import (
	"fmt"
	"mem/memtable"
)

func main() {

	i, j := memtable.LoadConfig("config.txt")
	var m memtable.Memtable
	if i == "hash" {
		m = memtable.CreateHashMemtable(j)
	} else {
		m = memtable.CreateHashMemtable(j)
	}
	m.AddElement("k1", []byte("aaa"))
	_, data := m.GetElement("k1")
	fmt.Printf("%s \n", data)
	m.DeleteElement("k1")
	_, data = m.GetElement("k1")
	fmt.Printf("%s \n", data)

}
