package cursor

import "scanning/lru"

type Cursor struct {
	memPointers []*interface{}
	memIndex    int
	lruPointer  *lru.LRUCache
}
