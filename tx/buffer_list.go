package tx

import (
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
)

// pinnedBuffer tracks the underlying buffer + how many times this transaction pinned it.
type pinnedBuffer struct {
	buffer   *buffer.Buffer
	refCount int
}

// BufferList manages a transaction's currently pinned buffers with reference counts.
type BufferList struct {
	buffers       map[file.BlockId]*pinnedBuffer
	bufferManager *buffer.Manager
}

// NewBufferList creates a new BufferList.
func NewBufferList(bufferManager *buffer.Manager) *BufferList {
	return &BufferList{
		buffers:       make(map[file.BlockId]*pinnedBuffer),
		bufferManager: bufferManager,
	}
}

// GetBuffer returns the buffer pinned to the specified block.
// The method returns nil if the transaction has not pinned the block.
func (bl *BufferList) GetBuffer(block *file.BlockId) *buffer.Buffer {
	pinnedBuf, ok := bl.buffers[*block]
	if !ok {
		return nil
	}
	return pinnedBuf.buffer
}

// Pin pins the block. If the block is already pinned by this transaction,
// simply increment the reference count. Otherwise, pin it via bufferManager.
func (bl *BufferList) Pin(block *file.BlockId) error {
	if pinnedBuf, ok := bl.buffers[*block]; ok {
		// Already pinned by this transaction; just increase refCount
		pinnedBuf.refCount++
		return nil
	}

	// Not pinned yet; ask bufferManager for a fresh pin
	buff, err := bl.bufferManager.Pin(block)
	if err != nil {
		return err
	}
	bl.buffers[*block] = &pinnedBuffer{
		buffer:   buff,
		refCount: 1,
	}
	return nil
}

// Unpin decrements the refCount. Only call bufferManager.Unpin when the last pin is released.
func (bl *BufferList) Unpin(block *file.BlockId) {
	pinnedBuf, ok := bl.buffers[*block]
	if !ok {
		// This block isn't pinned or was already unpinned.
		// In production, you might log a warning or return silently.
		return
	}
	pinnedBuf.refCount--
	if pinnedBuf.refCount <= 0 {
		// Now fully unpin from buffer manager and remove from our map
		bl.bufferManager.Unpin(pinnedBuf.buffer)
		delete(bl.buffers, *block)
	}
}

// UnpinAll unpins all blocks pinned by this transaction.
// We decrement each block's refCount down to zero, unpinning once for each pin.
func (bl *BufferList) UnpinAll() {
	for _, pinnedBuf := range bl.buffers {
		// We pinned this 'pinnedBuf.refCount' times; unpin that many times
		for pinnedBuf.refCount > 0 {
			pinnedBuf.refCount--
			bl.bufferManager.Unpin(pinnedBuf.buffer)
		}
		// Alternatively:
		//   for i := 0; i < pinnedBuf.refCount; i++ {
		//       bl.bufferManager.Unpin(pinnedBuf.buffer)
		//   }
		// pinnedBuf.refCount = 0
	}
	// Clear our map
	bl.buffers = make(map[file.BlockId]*pinnedBuffer)
}
