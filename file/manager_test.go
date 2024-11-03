package file

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestFileManager(t *testing.T) {
	// Test setup
	tempDir := filepath.Join(os.TempDir(), "db_test")
	blockSize := 400

	// Clean up after tests
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Errorf("Failed to clean up test directory: %v", err)
		}
	}()

	t.Run("AppendAndRead", func(t *testing.T) {
		assert := assert.New(t)
		mgr, err := NewManager(tempDir, blockSize)
		assert.NoErrorf(err, "Failed to create new manager: %v", err)

		// Create a test file and append a block
		filename := "test.db"
		block, err := mgr.Append(filename)
		assert.NoErrorf(err, "Failed to append block: %v", err)

		assert.Equalf(block.Number(), 0, "Expected first block number to be 0, got %d", block.Number())

		// Create a page with test data
		page := NewPage(blockSize)
		testData := "Hello, Database!"
		err = page.SetString(0, testData)
		assert.NoErrorf(err, "Error while putting string into page: %v", err)

		// Write the page to the block
		err = mgr.Write(&block, page)
		assert.NoErrorf(err, "Failed to write block: %v", err)

		// Read the page back
		readPage := NewPage(blockSize)
		err = mgr.Read(&block, readPage)
		assert.NoErrorf(err, "Failed to read block: %v", err)

		// Verify the contents
		readData, err := readPage.GetString(0)
		assert.NoErrorf(err, "Error while reading data from page: %v", err)
		assert.Equalf(readData, testData, "Expected %s, got %s", testData, readData)
	})

	t.Run("MultipleBlocks", func(t *testing.T) {
		assert := assert.New(t)

		mgr, err := NewManager(tempDir, blockSize)
		assert.NoErrorf(err, "Failed to create new manager: %v", err)

		filename := "multiblock.db"
		numBlocks := 5
		blocks := make([]BlockId, numBlocks)

		// Append multiple blocks
		for i := 0; i < numBlocks; i++ {
			block, err := mgr.Append(filename)
			assert.NoErrorf(err, "Failed to append block %d: %v", i, err)

			blocks[i] = block

			assert.Equalf(block.Number(), i, "Expected block number %d, got %d", i, block.Number())
		}

		// Write different data to each block
		for i, block := range blocks {
			page := NewPage(blockSize)
			data := fmt.Sprintf("Block %d data", i)
			err = page.SetString(0, data)
			assert.NoError(err)

			err := mgr.Write(&block, page)
			assert.NoErrorf(err, "Failed to write block %d: %v", i, err)
		}

		// Read and verify each block
		for i, block := range blocks {
			page := NewPage(blockSize)
			err := mgr.Read(&block, page)
			assert.NoErrorf(err, "Failed to read block %d: %v", i, err)

			expectedData := fmt.Sprintf("Block %d data", i)
			readData, err := page.GetString(0)
			assert.NoErrorf(err, "Error while reading string from page: %v", err)
			assert.Equalf(readData, expectedData, "Block %d: expected %s, got %s", i, expectedData, readData)
		}
	})

	t.Run("FileLength", func(t *testing.T) {
		assert := assert.New(t)

		mgr, err := NewManager(tempDir, blockSize)
		assert.NoErrorf(err, "Failed to create new manager: %v", err)

		filename := "length_test.db"
		numBlocks := 3

		// Append blocks
		for i := 0; i < numBlocks; i++ {
			_, err := mgr.Append(filename)
			assert.NoErrorf(err, "Failed to append block %d: %v", i, err)
		}

		// Check file length
		length, err := mgr.UnsafeLength(filename)
		assert.NoErrorf(err, "Failed to get file length: %v", err)

		assert.Equalf(length, numBlocks, "Expected length %d, got %d", numBlocks, length)
	})

	t.Run("TempFileCleanup", func(t *testing.T) {
		assert := assert.New(t)

		// Create a temporary file that should be cleaned up
		tempFile := filepath.Join(tempDir, "temp_test.db")
		err := os.WriteFile(tempFile, []byte("test data"), 0666)
		assert.NoErrorf(err, "Failed to create temp file: %v", err)

		// Create new manager which should clean up temp files
		_, err = NewManager(tempDir, blockSize)
		assert.NoErrorf(err, "Failed to create new manager: %v", err)

		// Check if temp file was removed
		_, err = os.Stat(tempFile)
		assert.ErrorIs(err, os.ErrNotExist, "Expected temp file to be removed")
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		assert := assert.New(t)

		mgr, err := NewManager(tempDir, blockSize)
		assert.NoErrorf(err, "Failed to create new manager: %v", err)

		filename := "concurrent.db"
		numGoroutines := 10
		numOperations := 5
		var wg sync.WaitGroup

		// Concurrent reads and writes
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Create a new block for this goroutine.
				block, err := mgr.Append(filename)
				assert.NoErrorf(err, "Failed to append initial block: %v", err)

				for j := 0; j < numOperations; j++ {
					page := NewPage(blockSize)
					data := fmt.Sprintf("Goroutine %d Operation %d", id, j)
					err = page.SetString(0, data)
					assert.NoError(err)

					// Write data
					err := mgr.Write(&block, page)
					assert.NoErrorf(err, "Goroutine %d write failed: %v", id, err)

					// Read data back
					readPage := NewPage(blockSize)
					err = mgr.Read(&block, readPage)
					assert.NoErrorf(err, "Goroutine %d read failed: %v", id, err)
					readData, err := readPage.GetString(0)
					assert.NoError(err)
					assert.Equal(data, readData)
				}
			}(i)
		}

		wg.Wait()
	})
}
