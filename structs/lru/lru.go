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

//func mainn() {
//	var config Config
//	configData, err := os.ReadFile("config.yaml")
//	if err != nil {
//		log.Fatal(err)
//	}
//	err = yaml.Unmarshal(configData, &config)
//	if err != nil {
//		log.Fatal(err)
//	}
//	lru := NewLRUCache(config.LruCap)
//	x1 := datatype.CreateDataType("kljuc1", []byte("vrednost1"))
//
//	lru.Put(x1)
//	lru.Put(datatype.CreateDataType("kljuc2", []byte("vrednost2")))
//	lru.Put(datatype.CreateDataType("kljuc3", []byte("vrednost3")))
//	lru.Put(datatype.CreateDataType("kljuc4", []byte("vrednost4")))
//	lru.Delete("kljuc3")
//	proba := lru.GetAll()
//	for e := proba.Front(); e != nil; e = e.Next() {
//		fmt.Println(e.Value.(*datatype.DataType).GetKey())
//	}
//	//fmt.Println(config.LruCap)
//}
