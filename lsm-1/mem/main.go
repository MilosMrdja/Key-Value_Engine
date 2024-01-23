package main

import (
	"sstable/mem/memtable/btree/btreemem"
	"sstable/mem/memtable/hash/hashstruct"
	"sstable/mem/memtable/skiplist/skiplistmem"
)

func main() {
	j := 10

	mem := hashstruct.CreateHashMemtable(j)
	mem.AddElement("1", []byte("1"))
	mem.AddElement("2", []byte("2"))
	mem.AddElement("3", []byte("3"))
	mem.AddElement("4", []byte("4"))
	mem.AddElement("7", []byte("7"))
	mem.AddElement("8", []byte("8"))
	mem.AddElement("9", []byte("9"))
	mem.AddElement("5", []byte("5"))
	mem.AddElement("6", []byte("6"))
	mem.AddElement("10", []byte("10"))
	//datalist := mem.SendToSSTable()
	slmem := skiplistmem.CreateSkipListMemtable(j)
	slmem.AddElement("1", []byte("1"))
	slmem.AddElement("2", []byte("2"))
	slmem.AddElement("3", []byte("3"))
	slmem.AddElement("4", []byte("4"))
	slmem.AddElement("7", []byte("7"))
	slmem.AddElement("8", []byte("8"))
	slmem.AddElement("9", []byte("9"))
	slmem.AddElement("5", []byte("5"))
	slmem.AddElement("6", []byte("6"))
	slmem.AddElement("10", []byte("10"))

	//datalist = slmem.SendToSSTable()

	btmem := btreemem.NewBTreeMemtable(j)
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

	//found, elem := btmem.GetElement("10")
	//if found {
	//	fmt.Printf("%s \n", elem)
	//}
	//
	//found, elem = btmem.GetElement("100001")
	//if found {
	//	fmt.Printf("%s \n", elem)
	//}

	//datalist = btmem.SendToSSTable()
	//for i := 0; i < len(datalist); i++ {
	//	fmt.Printf("%s ", datalist[i].GetData())
	//	fmt.Printf("\n")
	//}

	btmem.DeleteElement("10")
	btmem.SendToSSTable()

}
