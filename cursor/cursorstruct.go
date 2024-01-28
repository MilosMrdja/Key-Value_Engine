package cursor

import "lru"

type Cursor struct {
	memPointers []*interface{}
	memIndex    int
	lruPointer  *lru.LRUCache
}
