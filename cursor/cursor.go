package cursor

import "KeyValueEngine/lru"

type Cursor struct {
	memPointers []*interface{}
	memIndex    int
	lruPointer  lru.LRUCache
}
