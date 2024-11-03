package log

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"sync"
)

type Manager struct {
	fileManager  *file.Manager
	logFile      string
	logPage      *file.Page
	currentBlock *file.BlockId
	latestLSN    int
	lastSavedLSN int
	mu           sync.Mutex
}

// NewManager creates the manager for the specified log file.
// If the log file does not yet exist, it is created with an empty first block.
func NewManager(fileManager *file.Manager, logFile string) (*Manager, error) {
	// Create a new empty page.
	logPage := file.NewPage(fileManager.BlockSize())
	// Get the number of blocks in the log file. No need to take a lock here since this file is only accessed by the log
	// manager (and there is only one instance of the log manager).
	logSize, err := fileManager.UnsafeLength(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to get log file length: %v", err)
	}

	var currentBlock *file.BlockId
	if logSize == 0 {
		// If the log file is empty, append a new empty block to it.
		currentBlock, err = appendNewBlock(fileManager, logFile, logPage)
		if err != nil {
			return nil, fmt.Errorf("failed to append new block: %v", err)
		}
	} else {
		// If the log file is not empty, read the last block into the page.
		currentBlock = &file.BlockId{File: logFile, BlockNumber: logSize - 1}
		if err := fileManager.Read(currentBlock, logPage); err != nil {
			return nil, fmt.Errorf("failed to read log page: %v", err)
		}
	}

	return &Manager{
		fileManager:  fileManager,
		logFile:      logFile,
		logPage:      logPage,
		currentBlock: currentBlock,
		latestLSN:    0,
	}, nil
}

func (m *Manager) Flush(lsn int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lsn >= m.lastSavedLSN {
		return m.UnsafeFlush()
	}
	return nil
}

// Iterator returns an iterator over the log records.
func (m *Manager) Iterator() (*Iterator, error) {
	if err := m.UnsafeFlush(); err != nil {
		return nil, fmt.Errorf("failed to flush log: %v", err)
	}
	return NewIterator(m.fileManager, m.currentBlock)
}

// Append appends a log record to the log buffer.
// The record consists of an arbitrary byte slice.
// Log records are written from right to left in the buffer.
// The size of the record is written before the bytes.
// The beginning of the buffer contains the location of the last-written record (the "boundary").
// Storing the records backwards makes it easy to read them in reverse order.
// Returns the LSN of the final value.
//                                 * boundary
// [<boundary (int)>............[][recordN (bytes)]...[record1 (bytes)]]
func (m *Manager) Append(logRecord []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get the current boundary.
	boundary := int(m.logPage.GetInt(0))

	recordSize := len(logRecord)
	bytesNeeded := recordSize + 4 // 4 bytes for the integer storing the record size.

	if boundary-bytesNeeded < 4 { // The first 4 bits are for the boundary value.
		// The page doesn't have enough space.
		// Flush the page to disk.
		if err := m.UnsafeFlush(); err != nil {
			return 0, fmt.Errorf("failed to flush log: %v", err)
		}

		// Allocate a new block on the log.
		var err error
		m.currentBlock, err = appendNewBlock(m.fileManager, m.logFile, m.logPage)
		if err != nil {
			return 0, fmt.Errorf("failed to append new block: %v", err)
		}

		// Load the new boundary.
		boundary = int(m.logPage.GetInt(0))
	}

	recordPosition := boundary - bytesNeeded

	// Write the record.
	m.logPage.SetBytes(recordPosition, logRecord)
	// Update the boundary.
	m.logPage.SetInt(0, int32(recordPosition))

	m.latestLSN++
	return m.latestLSN, nil
}

// appendNewBlock initializes the byte buffer and appends it to the log file.
func appendNewBlock(fileManager *file.Manager, logFile string, logPage *file.Page) (*file.BlockId, error) {
	// Add an empty block to the log file.
	block, err := fileManager.Append(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to append new block: %v", err)
	}

	// Set the initial boundary for the page, every time we flush the page we reset its contents. This is done by
	// resetting the boundary. The initial value for the boundary is the `blockSize`, which represents the last bit of
	// the page (since the page is of size `blockSize`).
	logPage.SetInt(0, int32(fileManager.BlockSize()))
	if err := fileManager.Write(&block, logPage); err != nil {
		return nil, fmt.Errorf("failed to write new block: %v", err)
	}
	return &block, nil
}

// UnsafeFlush writes the buffer to the log file. This method is not thread-safe.
func (m *Manager) UnsafeFlush() error {
	if err := m.fileManager.Write(m.currentBlock, m.logPage); err != nil {
		return fmt.Errorf("failed to write log page: %v", err)
	}
	m.lastSavedLSN = m.latestLSN
	return nil
}
