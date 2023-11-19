package memtable

import (
	"math/rand"
)

type SkiplistNode struct {
	key  string
	data *DataType
	next []*SkiplistNode
}

func CreateSkiplistNode(key string, data []byte, level int) *SkiplistNode {
	return &SkiplistNode{
		key:  key,
		data: CreateDataType(data),
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
		head:     CreateSkiplistNode("", nil, maxLevel),
		level:    0,
		maxLevel: maxLevel,
	}
}

func (sl *SkipList) Insert(key string, data []byte) bool {
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

		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}

		update[i] = current
	}

	current = current.next[0]

	if current == nil || current.key != key {
		newNode := CreateSkiplistNode(key, data, sl.level)

		for i := 0; i <= newLevel; i++ {
			newNode.next[i] = update[i].next[i]
			update[i].next[i] = newNode
		}

		return true
	} else {
		return false
	}
}

func (sl *SkipList) DeleteElement(key string) bool {
	current := sl.head
	for i := sl.level; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	current = current.next[0]

	if current != nil && current.key == key {
		current.data.DeleteDataType()
		return true
	}
	return false
}

func (sl *SkipList) GetElement(key string) (bool, *DataType) {
	current := sl.head
	for i := sl.level; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	current = current.next[0]

	if current != nil && current.key == key {
		current.data.DeleteDataType()
		return true, current.data
	}
	return false, current.data
}
