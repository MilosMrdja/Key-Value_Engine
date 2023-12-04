package main

import (
	"fmt"
	"mem/memtable/btree/btreemem"
	"mem/memtable/hash/hashmem"
	"mem/memtable/hash/hashstruct"
	"mem/memtable/skiplist/skiplistmem"
)

func main() {

	i, j := hashmem.LoadConfig("config.txt")
	var m hashmem.Memtable
	if i == "hash" {
		m = hashstruct.CreateHashMemtable(j)
	} else {
		m = hashstruct.CreateHashMemtable(j)
	}
	m.AddElement("k1", []byte("aaa"))
	_, data := m.GetElement("k1")
	fmt.Printf("%s \n", data)
	m.DeleteElement("k1")
	_, data = m.GetElement("k1")
	fmt.Printf("%s \n", data)

	j = 4
	slmem := skiplistmem.CreateSkipListMemtable(j)
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

	btmem := btreemem.NewBTreeMemtable(10)
	btmem.AddElement("1", []byte("1"))
	btmem.AddElement("2", []byte("2"))
	btmem.AddElement("3", []byte("3"))
	btmem.AddElement("4", []byte("4"))
	btmem.AddElement("7", []byte("7"))
	btmem.AddElement("8", []byte("8"))
	btmem.AddElement("9", []byte("9"))
	btmem.AddElement("5", []byte("5"))
	btmem.AddElement("6", []byte("6"))
	btmem.AddElement("10", []byte("10"))

	found, elem := btmem.GetElement("10")
	if found {
		fmt.Printf("%s \n", elem)
	}

	found, elem = btmem.GetElement("100001")
	if found {
		fmt.Printf("%s \n", elem)
	}

}
