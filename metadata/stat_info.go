package metadata

type StatInfo struct {
	numBlocks      int
	numRecords     int
	distinctValues map[string]int
}

// NewStatInfo creates a new StatInfo object with calculated distinct values.
func NewStatInfo(numBlocks, numRecords int, distinctValues map[string]int) *StatInfo {
	return &StatInfo{
		numBlocks:      numBlocks,
		numRecords:     numRecords,
		distinctValues: distinctValues,
	}
}

// BlocksAccessed returns the estimated number of blocks in the table.
func (si *StatInfo) BlocksAccessed() int {
	return si.numBlocks
}

// RecordsOutput returns the estimated number of records in the table.
func (si *StatInfo) RecordsOutput() int {
	return si.numRecords
}

// DistinctValues returns the estimated number of distinct values for a given field in the table.
// Returns -1 if the field is not found.
func (si *StatInfo) DistinctValues(fieldName string) int {
	if val, ok := si.distinctValues[fieldName]; ok {
		return val
	}
	return -1 // Default to -1 if the field is not found
}
