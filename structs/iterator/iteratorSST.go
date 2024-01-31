package iterator

import "sstable/SSTableStruct/SSTable"

type IteratorSSTable struct {
	positionInSSTable map[*SSTable.SSTable]int32 // kljuc = pokazivac sstabelu, vrednost = do kog elementa
	prefix            string
	rang              string // aa - bb?
}

// mozda provera ako je mapa == nil, da se napravi sa make samo prilikom prvog poziva
func (i *IteratorSSTable) setSSTable(table *SSTable.SSTable) {
	i.positionInSSTable[table] = 0
}

func (i *IteratorSSTable) getSSTableMap() map[*SSTable.SSTable]int32 {
	return i.positionInSSTable
}

// treba provera kada dodjemo do maks el, ali to moze i van funkcije pozivom get offset
func (i *IteratorSSTable) IncrementElementOffset(table *SSTable.SSTable) {
	i.positionInSSTable[table]++
}

func (i *IteratorSSTable) GetOffsetEl(table *SSTable.SSTable) int32 {
	return i.positionInSSTable[table]
}

func (i *IteratorSSTable) getPrefix() string {
	return i.prefix
}

func (i *IteratorSSTable) setPrefix(p string) {
	i.prefix = p
}

func (i *IteratorSSTable) getRange() string {
	return i.rang
}

func (i *IteratorSSTable) setRange(r string) {
	i.rang = r
}
