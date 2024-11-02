package file

import "fmt"

type BlockId struct {
	filename    string
	blockNumber int
}

func NewBlockId(filename string, blockNumber int) *BlockId {
	return &BlockId{
		filename:    filename,
		blockNumber: blockNumber,
	}
}

func (b *BlockId) Filename() string {
	return b.filename
}

func (b *BlockId) Number() int {
	return b.blockNumber
}

func (b *BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blockNumber)
}
