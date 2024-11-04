package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Manager is the File Manager used by the database.
type Manager struct {
	dbDirectory   string
	blockSize     int
	isNew         bool
	mu            sync.Mutex
	openFiles     map[string]*os.File
	blocksRead    int
	blocksWritten int
}

// NewManager instantiates a new File Manager. Creates a new database directory if one doesn't already exist.
func NewManager(dbDirectory string, blockSize int) (*Manager, error) {
	isNew := false

	if _, err := os.Stat(dbDirectory); os.IsNotExist(err) {
		isNew = true
		if err := os.MkdirAll(dbDirectory, 0755); err != nil {
			return nil, fmt.Errorf("cannot create directory %s: %v", dbDirectory, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("cannot access directory %s: %v", dbDirectory, err)
	}

	// Remove any leftover temporary tables.
	entries, err := os.ReadDir(dbDirectory)
	if err != nil {
		return nil, fmt.Errorf("cannot read directory %s: %v", dbDirectory, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			name := entry.Name()
			if len(name) >= 4 && name[:4] == "temp" {
				tempFilePath := filepath.Join(dbDirectory, name)
				if err := os.Remove(tempFilePath); err != nil {
					return nil, fmt.Errorf("cannot remove file %s: %v", tempFilePath, err)
				}
			}
		}
	}

	return &Manager{
		dbDirectory:   dbDirectory,
		blockSize:     blockSize,
		isNew:         isNew,
		openFiles:     make(map[string]*os.File),
		blocksRead:    0,
		blocksWritten: 0,
	}, nil
}

// Read reads a block from the file into the Page.
func (m *Manager) Read(block *BlockId, page *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getFile(block.Filename())
	if err != nil {
		return fmt.Errorf("cannot read block %s: %v", block.String(), err)
	}

	offset := int64(block.Number()) * int64(m.blockSize)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek to offset %d: %v", offset, err)
	}

	buf := page.Contents()
	n, err := io.ReadFull(f, buf)

	// Handle successful read
	if err == nil && n == len(buf) {
		m.blocksRead++
		return nil
	}

	// Handle EOF case
	if errors.Is(err, io.EOF) {
		// File was empty.
		if n == 0 {
			m.blocksRead++
			return nil
		}
		// File wasn't empty, but encountered unexpected EOF.
		return fmt.Errorf("partial read at EOF: expected %d bytes, got %d", len(buf), n)
	}

	// Handle other errors
	if err != nil {
		return fmt.Errorf("cannot read data: %v", err)
	}

	// Handle short read (should be unreachable with io.ReadFull)
	return fmt.Errorf("short read: expected %d bytes, got %d", len(buf), n)
}

func (m *Manager) Write(block *BlockId, page *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getFile(block.Filename())
	if err != nil {
		return fmt.Errorf("cannot write block %s: %v", block.String(), err)
	}

	offset := int64(block.Number()) * int64(m.blockSize)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek to offset %d: %v", offset, err)
	}

	buf := page.Contents()
	n, err := f.Write(buf)
	if err != nil {
		if n != len(buf) {
			return fmt.Errorf("short write: expected %d bytes, wrote %d, %v", len(buf), n, err)
		}
		return fmt.Errorf("cannot write data: %v", err)
	}

	// Ensure the data is flushed to disk.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("cannot flush file %s to disk: %v", block.Filename(), err)
	}

	m.blocksWritten++
	return nil
}

// Append appends a new block to the file and returns its BlockId.
func (m *Manager) Append(filename string) (BlockId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newBlockNumber, err := m.Length(filename)
	if err != nil {
		return BlockId{}, fmt.Errorf("cannot get length of %s: %v", filename, err)
	}

	block := BlockId{File: filename, BlockNumber: newBlockNumber}

	f, err := m.getFile(filename)
	if err != nil {
		return BlockId{}, fmt.Errorf("cannot append block %s: %v", block.String(), err)
	}

	offset := int64(block.Number()) * int64(m.blockSize)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return BlockId{}, fmt.Errorf("cannot seek to offset %d: %v", offset, err)
	}

	b := make([]byte, m.blockSize)
	n, err := f.Write(b)
	if err != nil {
		return BlockId{}, fmt.Errorf("cannot write data: %v", err)
	}
	if n != len(b) {
		return BlockId{}, fmt.Errorf("short write: expected %d bytes, wrote %d", len(b), n)
	}

	// Ensure the data is flushed to disk.
	if err := f.Sync(); err != nil {
		return BlockId{}, fmt.Errorf("cannot sync file %s: %v", filename, err)
	}

	m.blocksWritten++

	return block, nil
}

// Length returns the number of blocks in the specified file. This method is not thread-safe.
func (m *Manager) Length(filename string) (int, error) {
	f, err := m.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("cannot access %s: %v", filename, err)
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("cannot stat %s: %v", filename, err)
	}

	fileSizeInBytes := fileInfo.Size()
	return int(fileSizeInBytes / int64(m.blockSize)), nil
}

// IsNew returns true if the database directory is newly created.
func (m *Manager) IsNew() bool {
	return m.isNew
}

// BlockSize returns the block size used by the FileMgr.
func (m *Manager) BlockSize() int {
	return m.blockSize
}

// getFile retrieves or opens a file and stores it in the openFiles map.
func (m *Manager) getFile(filename string) (*os.File, error) {
	if f, ok := m.openFiles[filename]; ok {
		return f, nil
	}

	dbTable := filepath.Join(m.dbDirectory, filename)
	f, err := os.OpenFile(dbTable, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0666)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %s: %v", dbTable, err)
	}

	m.openFiles[filename] = f
	return f, nil
}

// GetBlocksRead returns the total number of blocks read.
func (m *Manager) GetBlocksRead() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.blocksRead
}

// GetBlocksWritten returns the total number of blocks written.
func (m *Manager) GetBlocksWritten() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.blocksWritten
}
