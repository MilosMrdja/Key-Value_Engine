package iterator

import (
	"sstable/mem/memtable/datatype"
)

type IteratingCache struct {
	currentPosition int
	maxNum          int
	iterCache       []datatype.DataType
}

func (i *IteratingCache) CurrentPosition() int {
	return i.currentPosition
}

func (i *IteratingCache) SetCurrentPosition(currentPosition int) {
	i.currentPosition = currentPosition
}

func (i *IteratingCache) IterCache() []datatype.DataType {
	return i.iterCache
}
func (i *IteratingCache) CurrentElement() datatype.DataType {
	return i.iterCache[i.CurrentPosition()]
}

func (i *IteratingCache) DecrementPosition() {
	if i.currentPosition == i.maxNum {
		i.currentPosition--
		return
	}
	element := i.CurrentElement()
	if i.currentPosition != 0 || element.GetKey() != "" {
		i.currentPosition--
	}
}

func (i *IteratingCache) IncrementPosition() {
	if i.CurrentPosition() != i.maxNum {
		i.currentPosition++
	} else {
		i.currentPosition = i.maxNum
	}
}
func (i *IteratingCache) CheckIfEnd() bool {
	element := i.CurrentElement()
	return i.CurrentPosition() == 0 || element.GetKey() == ""
}
func (i *IteratingCache) CheckIfLast() bool {
	return i.currentPosition == i.maxNum
}

func (i *IteratingCache) InsertCache(elem datatype.DataType) {
	i.iterCache = i.IterCache()[1:]
	i.iterCache = append(i.iterCache, elem)
}

func NewIteratingCache(numSaved int) *IteratingCache {
	cache := make([]datatype.DataType, numSaved, numSaved)

	return &IteratingCache{iterCache: cache, maxNum: numSaved, currentPosition: numSaved}
}

// ==================================================================================================
type PageCache struct {
}
