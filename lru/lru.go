package main

import (
	"container/list"
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

type Node struct {
	key  string
	data []byte
}

type LRUCache struct {
	cap       int
	cache     map[string]*list.Element
	cacheList *list.List
}

func (l *LRUCache) Put(key string, data []byte) {
	if ele, ok := l.cache[key]; ok {
		l.cacheList.Remove(ele)
	}
	ele2 := l.cacheList.PushBack(NewNode(key, data))
	l.cache[key] = ele2
	if l.cacheList.Len() > l.cap {
		leastRU := l.cacheList.Front()
		l.cacheList.Remove(leastRU)
		delete(l.cache, leastRU.Value.(*Node).key)
	}
}
func (l *LRUCache) Get(key string) []byte {
	if ele, ok := l.cache[key]; ok {
		temp := l.cache[key].Value.(*Node)
		l.cacheList.Remove(ele)
		ele2 := l.cacheList.PushBack(temp)
		l.cache[key] = ele2
		return l.cache[key].Value.(*Node).data
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

func NewNode(k string, d []byte) *Node {
	return &Node{
		key:  k,
		data: d,
	}
}

type Config struct {
	LruCap int `yaml:"lru_cap"`
}

func main() {
	var config Config
	configData, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	err = yaml.Unmarshal(configData, &config)
	if err != nil {
		log.Fatal(err)
	}
	lru := NewLRUCache(config.LruCap)
	lru.Put("kljuc1", []byte("vrednost1"))
	lru.Put("kljuc2", []byte("vrednost2"))
	lru.Put("kljuc3", []byte("vrednost3"))
	lru.Put("kljuc4", []byte("vrednost4"))
	lru.Delete("kljuc3")
	proba := lru.GetAll()
	for e := proba.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
	//fmt.Println(config.LruCap)
}
