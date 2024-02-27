package skipliststruct

import (
	"fmt"
	"math/rand"
	"sstable/mem/memtable/datatype"
	"strings"
	"time"
)

type SkiplistNode struct {
	key  string
	data *datatype.DataType
	next []*SkiplistNode
}

func isInRange(value string, valRange []string) bool {
	return value >= valRange[0] && value <= valRange[1]
}

func CreateSkiplistNode(data *datatype.DataType, level int) *SkiplistNode {
	return &SkiplistNode{
		key:  data.GetKey(),
		data: data,
		next: make([]*SkiplistNode, level+1),
	}
}

type SkipList struct {
	head     *SkiplistNode
	level    int
	maxLevel int
}

func CreateSkipList(maxLevel int) *SkipList {
	return &SkipList{
		head:     CreateSkiplistNode(datatype.CreateDataType("", []byte(""), time.Now()), 0),
		level:    0,
		maxLevel: maxLevel,
	}
}

func (sl *SkipList) Insert(data *datatype.DataType) bool {
	found, elem := sl.GetElement(data.GetKey())
	if found == false {
		newLevel := 0

		for newLevel < sl.level+1 && rand.Intn(2) == 1 {
			newLevel++
		}

		if sl.level < newLevel {
			sl.head.next = append(sl.head.next, make([]*SkiplistNode, newLevel-sl.level)...)
			sl.level = newLevel
		}

		current := sl.head
		update := make([]*SkiplistNode, sl.level+1)

		for i := sl.level; i >= 0; i-- {

			for current.next[i] != nil && current.next[i].key < data.GetKey() {
				current = current.next[i]
			}

			update[i] = current
		}

		current = current.next[0]

		if current == nil || current.key != data.GetKey() {
			newNode := CreateSkiplistNode(data, sl.level)

			for i := 0; i <= newLevel; i++ {
				newNode.next[i] = update[i].next[i]
				update[i].next[i] = newNode
			}

			return true
		} else {
			return false
		}
	} else {
		elem.UpdateDataType(data.GetData(), data.GetChangeTime())
	}

	return true
}

func (sl *SkipList) DeleteElement(key string, time time.Time) bool {
	current := sl.head
	for i := sl.level; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	current = current.next[0]

	if current != nil && current.key == key {
		current.data.DeleteDataType(time)
		return true
	}
	return false
}

func (sl *SkipList) GetElement(key string) (bool, *datatype.DataType) {
	current := sl.head
	for i := sl.level; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	current = current.next[0]

	if current != nil && current.key == key {
		return true, current.data
	}
	return false, nil
}

func (sl *SkipList) ShowSkipList() {
	fmt.Println("\n")
	ranks := make(map[string]int)
	i := 0

	for node := sl.head.next[0]; node != nil; node = node.next[0] {
		ranks[node.key] = i
		i++
	}

	for level := sl.level; level >= 0; level-- {
		if sl.head.next[level] == nil {
			continue
		}
		i = 0
		for node := sl.head.next[level]; node != nil; node = node.next[level] {
			rank := ranks[node.key]
			for j := 0; j < rank-i; j++ {
				fmt.Print("--")
			}
			fmt.Print(node.key + "-")
			i = rank + 1
		}
		fmt.Print("\n")
	}
	fmt.Println("")
}

func (sl *SkipList) GetByPrefix(dataList []*datatype.DataType, n *int, prefix string) {

	i := 0
	current := sl.head.next[0]
	for current != nil {
		if strings.HasPrefix(current.key, prefix) {
			if *n == 0 {
				return
			}
			dataList = append(dataList, current.data)
			*n--
		}
		current = current.next[0]
		i++
	}

}
func (sl *SkipList) GetByRange(dataList []*datatype.DataType, n *int, valRange []string) {

	i := 0
	current := sl.head.next[0]
	for current != nil {
		if isInRange(current.key, valRange) {
			if *n == 0 {
				return
			}
			dataList = append(dataList, current.data)
			*n--
		}
		current = current.next[0]
		i++
	}

}
func (sl *SkipList) AllData(len int) []datatype.DataType {
	dataList := make([]datatype.DataType, len)
	i := 0
	current := sl.head.next[0]
	for current != nil {
		dataList[i] = *current.data
		current = current.next[0]
		i++
	}
	return dataList
}
