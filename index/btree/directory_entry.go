package btree

type DirectoryEntry struct {
	dataValue   any
	blockNumber int
}

// NewDirectoryEntry creates a new DirectoryEntry with the specified data value and block number.
func NewDirectoryEntry(dataValue any, blockNumber int) *DirectoryEntry {
	return &DirectoryEntry{dataValue, blockNumber}
}

// DataValue returns the data value of this directory entry.
func (de *DirectoryEntry) DataValue() any {
	return de.dataValue
}

// BlockNumber returns the block number of this directory entry.
func (de *DirectoryEntry) BlockNumber() int {
	return de.blockNumber
}
