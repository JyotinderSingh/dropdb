package tx

import (
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
)

// BufferList manages a transaction's currently pinned buffers.
type BufferList struct {
	buffers       map[file.BlockId]*buffer.Buffer
	pins          []*file.BlockId
	bufferManager *buffer.Manager
}

// NewBufferList creates a new BufferList.
func NewBufferList(bufferManager *buffer.Manager) *BufferList {
	return &BufferList{
		buffers:       make(map[file.BlockId]*buffer.Buffer),
		pins:          make([]*file.BlockId, 0, 10),
		bufferManager: bufferManager,
	}
}

// GetBuffer returns the buffer pinned to the specified block.
// The method returns nil if the transaction has not pinned the block.
func (bl *BufferList) GetBuffer(block *file.BlockId) *buffer.Buffer {
	return bl.buffers[*block]
}

// Pin pins the block and keeps track of the buffer internally.
func (bl *BufferList) Pin(block *file.BlockId) error {
	buff, err := bl.bufferManager.Pin(block)
	if err != nil {
		return err
	}
	bl.buffers[*block] = buff
	bl.pins = append(bl.pins, block)
	return nil
}

// Unpin unpins the block and removes it from the internal list of pinned buffers.
func (bl *BufferList) Unpin(block *file.BlockId) {
	bl.bufferManager.Unpin(bl.buffers[*block])
	delete(bl.buffers, *block)
	for i, b := range bl.pins {
		if *b == *block {
			bl.pins = append(bl.pins[:i], bl.pins[i+1:]...)
			break
		}
	}
}

// UnpinAll unpins all the blocks and clears the internal list of pinned buffers.
func (bl *BufferList) UnpinAll() {
	for _, block := range bl.pins {
		bl.bufferManager.Unpin(bl.buffers[*block])
	}
	bl.buffers = make(map[file.BlockId]*buffer.Buffer)
	bl.pins = make([]*file.BlockId, 0, 10)
}
