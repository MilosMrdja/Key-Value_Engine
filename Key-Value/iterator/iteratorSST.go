package iterator

type IteratorPrefixSSTable struct {
	PositionInSSTable map[string][]uint64 // kljuc = pokazivac sstabelu, vrednost = do kog elementa
	Prefix            string
}

func (i *IteratorPrefixSSTable) GetSSTableMap() map[string][]uint64 {
	return i.PositionInSSTable
}

// treba provera kada dodjemo do maks el, ali to moze i van funkcije pozivom get offset
func (i *IteratorPrefixSSTable) IncrementElementOffset(table string, off uint64) {
	i.PositionInSSTable[table][0] += off
}

func (i *IteratorPrefixSSTable) GetOffsetEl(table string) []uint64 {
	return i.PositionInSSTable[table]
}

func (i *IteratorPrefixSSTable) getPrefix() string {
	return i.Prefix
}

func (i *IteratorPrefixSSTable) setPrefix(p string) {
	i.Prefix = p
}

// iterator za scan
type IteratorRangeSSTable struct {
	PositionInSSTable map[string][]uint64 // kljuc = putanja do sstable, [0] = curr, [1] = end
	Rang              [2]string
}

func (i *IteratorRangeSSTable) getSSTableMap() map[string][]uint64 {
	return i.PositionInSSTable
}

// treba provera kada dodjemo do maks el, ali to moze i van funkcije pozivom get offset
func (i *IteratorRangeSSTable) IncrementElementOffset(table string, off uint64) {
	i.PositionInSSTable[table][0] += off
}

func (i *IteratorRangeSSTable) GetOffsetEl(table string) []uint64 {
	return i.PositionInSSTable[table]
}

func (i *IteratorRangeSSTable) getPrefix() [2]string {
	return i.Rang
}

func (i *IteratorRangeSSTable) setPrefix(p [2]string) {
	i.Rang = p
}
