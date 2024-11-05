package file

import "fmt"

// BlockId identifies a disk block by its filename and block number.
type BlockId struct {
	File        string
	BlockNumber int
}

func NewBlockId(filename string, blockNumber int) *BlockId {
	return &BlockId{
		File:        filename,
		BlockNumber: blockNumber,
	}
}

func (b *BlockId) Filename() string {
	return b.File
}

func (b *BlockId) Number() int {
	return b.BlockNumber
}

func (b *BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.File, b.BlockNumber)
}

func (b *BlockId) Equals(other *BlockId) bool {
	return b.File == other.File && b.BlockNumber == other.BlockNumber
}
