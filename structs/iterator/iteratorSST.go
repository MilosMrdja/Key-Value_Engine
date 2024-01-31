package iterator

type IteratorSSTable struct {
	PositionInSSTable map[string][]int64 // kljuc = pokazivac sstabelu, vrednost = do kog elementa
	Prefix            string
}

func (i *IteratorSSTable) getSSTableMap() map[string][]int64 {
	return i.PositionInSSTable
}

// treba provera kada dodjemo do maks el, ali to moze i van funkcije pozivom get offset
func (i *IteratorSSTable) IncrementElementOffset(table string) {
	i.PositionInSSTable[table][0]++
}

func (i *IteratorSSTable) GetOffsetEl(table string) []int64 {
	return i.PositionInSSTable[table]
}

func (i *IteratorSSTable) getPrefix() string {
	return i.Prefix
}

func (i *IteratorSSTable) setPrefix(p string) {
	i.Prefix = p
}
