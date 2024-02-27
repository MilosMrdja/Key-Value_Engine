package lru

import (
	"container/list"
	"sstable/mem/memtable/datatype"
)

type LRUCache struct {
	cap       int
	cache     map[string]*list.Element
	cacheList *list.List
}

func (l *LRUCache) Put(data *datatype.DataType) {
	if ele, ok := l.cache[data.GetKey()]; ok {
		l.cacheList.Remove(ele)
	}
	ele2 := l.cacheList.PushBack(data)
	l.cache[data.GetKey()] = ele2
	if l.cacheList.Len() > l.cap {
		leastRU := l.cacheList.Front()
		l.cacheList.Remove(leastRU)
		delete(l.cache, leastRU.Value.(*datatype.DataType).GetKey()) // dataype.getkey
	}
}
func (l *LRUCache) Get(key string) []byte {
	if ele, ok := l.cache[key]; ok {
		temp := l.cache[key].Value.(*datatype.DataType)
		l.cacheList.Remove(ele)
		ele2 := l.cacheList.PushBack(temp)
		l.cache[key] = ele2
		return l.cache[key].Value.(*datatype.DataType).GetData()
	}
	return nil
}

func (l *LRUCache) GetAll() *list.List {
	return l.cacheList
}

func (l *LRUCache) Delete(key string) {
	if ele, ok := l.cache[key]; ok {
		l.cacheList.Remove(ele)
	}
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		cap:       capacity,
		cache:     make(map[string]*list.Element),
		cacheList: list.New(),
	}
}

type Config struct {
	LruCap int `yaml:"lru_cap"`
}

