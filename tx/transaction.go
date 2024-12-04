package tx

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"math"
	"sync"
)

const EndOfFile = -1

var (
	nextTxNum   = 0
	nextTxNumMu sync.Mutex
)

// nextTxNumber increments and returns the next transaction number.
func nextTxNumber() int {
	nextTxNumMu.Lock()
	defer nextTxNumMu.Unlock()
	nextTxNum++
	return nextTxNum
}

type Transaction struct {
	recoverManager     *RecoveryManager
	concurrencyManager *concurrency.Manager
	bufferManager      *buffer.Manager
	fileManager        *file.Manager
	txNum              int
	myBuffers          *BufferList
}

// NewTransaction creates a new Transaction and its associated recovery and concurrency managers.
// This method depends on the file, log, and buffer managers which it receives from the instantiating class.
// These objects are usually created during system initialization. Thus, this constructor cannot be called until either
// the DropDB#Init or DropDB#InitFileLogAndBufferManager methods are called.
func NewTransaction(fileManager *file.Manager, logManager *log.Manager, bufferManager *buffer.Manager) *Transaction {
	tx := &Transaction{
		fileManager:        fileManager,
		bufferManager:      bufferManager,
		txNum:              nextTxNumber(),
		concurrencyManager: concurrency.NewManager(),
		myBuffers:          NewBufferList(bufferManager),
	}
	tx.recoverManager = NewRecoveryManager(tx, tx.txNum, logManager, bufferManager)
	return tx
}

// Commit commits the current transaction.
// Flushes all modified buffers (and their log records),
// Writes and flushes a commit record to the log,
// Releases all the locks, and unpins any pinned buffers.
func (tx *Transaction) Commit() error {
	if err := tx.recoverManager.Commit(); err != nil {
		return err
	}
	fmt.Printf("Transaction %d committed\n", tx.txNum)
	tx.concurrencyManager.Release()
	tx.myBuffers.UnpinAll()
	return nil
}

// Rollback rolls back the current transaction.
// Undoes any modified values,
// Flushes those buffers,
// Writes and flushes a rollback record to the log,
// Releases all the locks, and unpins any pinned buffers.
func (tx *Transaction) Rollback() error {
	if err := tx.recoverManager.Rollback(); err != nil {
		return err
	}
	fmt.Printf("Transaction %d rolled back\n", tx.txNum)
	tx.concurrencyManager.Release()
	tx.myBuffers.UnpinAll()
	return nil
}

// Recover flushes all modified buffers to disk, then goes through the log, rolling back all uncommitted transactions.
// Finally, writes a quiescent checkpoint record to the log. This method is called during system startup, before any
// user transactions begin.
func (tx *Transaction) Recover() error {
	if err := tx.bufferManager.FlushAll(tx.txNum); err != nil {
		return err
	}
	if err := tx.recoverManager.Recover(); err != nil {
		return err
	}
	return nil
}

// Pin pins the specified block.
// The transaction manages the buffer for the client.
func (tx *Transaction) Pin(block *file.BlockId) error {
	return tx.myBuffers.Pin(block)
}

// Unpin unpins the specified block.
// The transaction looks up the buffer pinned to this block, and unpins it.
func (tx *Transaction) Unpin(block *file.BlockId) {
	tx.myBuffers.Unpin(block)
}

// GetInt returns the integer value stored at the specified offset of the specified block.
// The method first obtains an SLock on the block,
// then it calls the buffer to retrieve the value.
func (tx *Transaction) GetInt(block *file.BlockId, offset int) (int, error) {
	if err := tx.concurrencyManager.SLock(block); err != nil {
		return math.MinInt, err
	}
	buff := tx.myBuffers.GetBuffer(block)
	if buff == nil {
		return math.MinInt, fmt.Errorf("buffer for block %s not found", block)
	}
	return buff.Contents().GetInt(offset), nil
}

// GetString returns the string value stored at the specified offset of the specified block.
// The method first obtains an SLock on the block,
// then it calls the buffer to retrieve the value.
func (tx *Transaction) GetString(block *file.BlockId, offset int) (string, error) {
	if err := tx.concurrencyManager.SLock(block); err != nil {
		return "", err
	}
	buff := tx.myBuffers.GetBuffer(block)
	if buff == nil {
		return "", fmt.Errorf("buffer for block %s not found", block)
	}
	return buff.Contents().GetString(offset)
}

// SetInt stores an integer at the specified offset of the specified block.
// The method first obtains an XLock on the block.
// It then reads the current value at that offset,
// puts it into an update log record, and writes that record to the log.
// Finally, it calls the buffer to store the value,
// passing in the LSN of the log record and the transaction's ID.
func (tx *Transaction) SetInt(block *file.BlockId, offset int, val int, logIt bool) error {
	var err error
	if err = tx.concurrencyManager.XLock(block); err != nil {
		return err
	}
	buff := tx.myBuffers.GetBuffer(block)
	if buff == nil {
		return fmt.Errorf("buffer for block %s not found", block)
	}

	lsn := -1
	if logIt {
		if lsn, err = tx.recoverManager.SetInt(buff, offset, val); err != nil {
			return err
		}
	}

	page := buff.Contents()
	page.SetInt(offset, val)
	buff.SetModified(tx.txNum, lsn)
	return nil
}

// SetString stores a string at the specified offset of the specified block.
// The method first obtains an XLock on the block.
// It then reads the current value at that offset,
// puts it into an update log record, and writes that record to the log.
// Finally, it calls the buffer to store the value,
// passing in the LSN of the log record and the transaction's ID.
func (tx *Transaction) SetString(block *file.BlockId, offset int, val string, logIt bool) error {
	var err error
	if err = tx.concurrencyManager.XLock(block); err != nil {
		return err
	}
	buff := tx.myBuffers.GetBuffer(block)
	if buff == nil {
		return fmt.Errorf("buffer for block %s not found", block)
	}

	lsn := -1
	if logIt {
		if lsn, err = tx.recoverManager.SetString(buff, offset, val); err != nil {
			return err
		}
	}

	page := buff.Contents()
	if err = page.SetString(offset, val); err != nil {
		return err
	}
	buff.SetModified(tx.txNum, lsn)
	return nil
}

// Size returns the number of blocks in the specified file.
// This method first obtains an SLock on the "end of file" marker,
// before asking the file manager to return the file size.
// This is necessary to prevent another transaction from adding a block to the file
// while this transaction is counting the blocks and causing phantom reads.
func (tx *Transaction) Size(filename string) (int, error) {
	dummyBlock := file.NewBlockId(filename, EndOfFile)
	if err := tx.concurrencyManager.SLock(dummyBlock); err != nil {
		return -1, err
	}
	return tx.fileManager.Length(filename)
}

// Append appends a new block to the end of the specified file and returns a reference to it.
// This method first obtains an XLock on the "end of file" marker, before performing the append operation.
// This is necessary to prevent another transaction from reading the size of the file while this append is in progress.
// This helps prevent phantom reads.
func (tx *Transaction) Append(filename string) (*file.BlockId, error) {
	dummyBlock := file.NewBlockId(filename, EndOfFile)
	if err := tx.concurrencyManager.XLock(dummyBlock); err != nil {
		return nil, err
	}
	return tx.fileManager.Append(filename)
}

// BlockSize returns the size of a block in the database.
func (tx *Transaction) BlockSize() int {
	return tx.fileManager.BlockSize()
}

// AvailableBuffers returns the number of available (unpinned) buffers.
func (tx *Transaction) AvailableBuffers() int {
	return tx.bufferManager.Available()
}

// TxNum returns the transaction number.
func (tx *Transaction) TxNum() int {
	return tx.txNum
}
