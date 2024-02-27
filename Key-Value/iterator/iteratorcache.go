package iterator

import (
	"fmt"
	"sstable/mem/memtable/datatype"
)

type IteratingCache struct {
	currentPosition int
	maxNum          int
	iterCache       []datatype.DataType
}

func (i *IteratingCache) MaxNum() int {
	return i.maxNum
}

func (i *IteratingCache) SetMaxNum(maxNum int) {
	i.maxNum = maxNum
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
	pageSize int
	pageNum  int
	currPage int
	CacheArr [][]datatype.DataType
}

func (p *PageCache) CurrPage() []datatype.DataType {
	return p.CacheArr[p.currPage]
}

func (p *PageCache) CurrPageCursor() int {
	return p.currPage
}
func (p *PageCache) DecrementCurrPage() {
	if len(p.CacheArr) != 0 && p.currPage != 0 {
		p.currPage--
		return
	}

}

func (p *PageCache) IncrementCurrPage() {
	if p.currPage != p.pageNum-1 {
		p.currPage++
	} else {
		p.currPage = p.pageNum - 1
	}
}
func (p *PageCache) SetCurrPage(currPage int) {
	p.currPage = currPage
}

func (p *PageCache) PageSize() int {
	return p.pageSize
}

func (p *PageCache) SetPageSize(pageSize int) {
	p.pageSize = pageSize
}

func (p *PageCache) PageNum() int {
	return p.pageNum
}

func (p *PageCache) SetPageNum(pageNum int) {
	p.pageNum = pageNum
}

func (p *PageCache) OutputCurrPage() {
	page := p.CurrPage()
	for i := 0; i < len(page); i++ {
		fmt.Printf("Kljuc: %s\n", page[i].GetKey())
	}
}

func (p *PageCache) InsertPage(stranica []datatype.DataType) {
	if len(p.CacheArr) != p.pageNum {
		p.CacheArr = append(p.CacheArr, stranica)
	} else {
		p.CacheArr = p.CacheArr[1:]
		p.CacheArr = append(p.CacheArr, stranica)
	}
}

func NewPageCache(pageNum int) *PageCache {
	pageArr := make([][]datatype.DataType, 0, pageNum)

	return &PageCache{pageNum: pageNum, currPage: pageNum - 1, CacheArr: pageArr}
}
func (i *PageCache) CheckIfEnd() bool {

	return i.CurrPageCursor() == 0
}
func (i *PageCache) CheckIfLast() bool {
	return i.CurrPageCursor() == i.pageNum-1
}
