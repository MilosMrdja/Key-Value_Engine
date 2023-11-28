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

	j = 4
	slmem := memtable.CreateSkipListMemtable(j)
	slmem.AddElement("1", []byte("1"))
	slmem.AddElement("2", []byte("2"))
	slmem.AddElement("3", []byte("3"))
	slmem.AddElement("4", []byte("4"))
	slmem.ShowSkipList()
	slmem.AddElement("7", []byte("7"))
	slmem.AddElement("8", []byte("8"))
	slmem.AddElement("9", []byte("9"))
	slmem.AddElement("5", []byte("5"))
	slmem.ShowSkipList()
	slmem.AddElement("6", []byte("6"))
	slmem.AddElement("10", []byte("10"))
	slmem.ShowSkipList()

}
