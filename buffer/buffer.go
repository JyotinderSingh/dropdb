package buffer

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
)

// Buffer represents an individual buffer. A data buffer wraps a page
// and stores information about its status,
// such as the associated disk block,
// the number of times the buffer has been pinned,
// whether its contents have been modified,
// and if so, the id and lsn of the modifying transaction.
type Buffer struct {
	fileManager *file.Manager
	logManager  *log.Manager
	contents    *file.Page
	block       *file.BlockId
	pins        int
	txnNum      int
	lsn         int
}

func NewBuffer(fileManager *file.Manager, logManager *log.Manager) *Buffer {
	return &Buffer{
		fileManager: fileManager,
		logManager:  logManager,
		contents:    file.NewPage(fileManager.BlockSize()),
		block:       nil,
		pins:        0,
		txnNum:      -1,
		lsn:         -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

// Block returns a reference to the disk block allocated to the buffer.
func (b *Buffer) Block() *file.BlockId {
	return b.block
}

func (b *Buffer) SetModified(txnNum, lsn int) {
	b.txnNum = txnNum

	// If LSN is smaller than 0, it indicates that a log record was not generated for this update.
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// isPinned returns true if the buffer is currently pinned (that is, if it has a nonzero pin count).
func (b *Buffer) isPinned() bool {
	return b.pins > 0
}

func (b *Buffer) modifyingTxn() int {
	return b.txnNum
}

// assignToBlock reads the contents of the specified block into the contents of the buffer.
// If the buffer was dirty, then its previous contents are first written to disk.
func (b *Buffer) assignToBlock(block *file.BlockId) error {
	if err := b.flush(); err != nil {
		return fmt.Errorf("failed to flush buffer for block %s: %v", b.block.String(), err)
	}
	b.block = block
	if err := b.fileManager.Read(block, b.contents); err != nil {
		return fmt.Errorf("failed to read block %s to buffer: %v", block.String(), err)
	}
	b.pins = 0
	return nil
}

// flush writes the buffer to its disk block if it is dirty. The method first writes the log record to the log file,
// and then writes the contents of the buffer to disk.
func (b *Buffer) flush() error {
	if b.txnNum >= 0 {
		if err := b.logManager.Flush(b.lsn); err != nil {
			return fmt.Errorf("failed to flush log record for txn %d: %v", b.txnNum, err)
		}
		if err := b.fileManager.Write(b.block, b.contents); err != nil {
			return fmt.Errorf("failed to write block: %v", err)
		}
		b.txnNum = -1
	}
	return nil
}

// pin increases the buffer's pin count.
func (b *Buffer) pin() {
	b.pins++
}

// unpin decreases the buffer's pin count.
func (b *Buffer) unpin() {
	b.pins--
}
