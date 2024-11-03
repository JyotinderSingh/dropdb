package log

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// Helper function to create a new temporary FileMgr
func createTempFileMgr(blocksize int) (*file.Manager, func(), error) {
	tmpDir, err := os.MkdirTemp("", "filemgr_test")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp directory: %v", err)
	}

	fm, err := file.NewManager(tmpDir, blocksize)
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, nil, fmt.Errorf("failed to create FileMgr: %v", err)
	}

	cleanup := func() { os.RemoveAll(tmpDir) }
	return fm, cleanup, nil
}

func TestLogMgr_AppendAndIteratorConsistency(t *testing.T) {
	assert := assert.New(t)
	blockSize := 4096
	fm, cleanup, err := createTempFileMgr(blockSize)
	defer cleanup()
	assert.NoErrorf(err, "Error creating FileMgr: %v", err)

	logfile := "testlog"
	lm, err := NewManager(fm, logfile)
	assert.NoErrorf(err, "Error creating LogMgr: %v", err)

	// Append and flush multiple records, then verify consistency
	recordCount := 100
	records := make([][]byte, recordCount)
	for i := 0; i < recordCount; i++ {
		records[i] = []byte(fmt.Sprintf("log record %d", i+1))
		_, err := lm.Append(records[i])
		assert.NoErrorf(err, "Error appending record %d: %v", i+1, err)
	}

	// Verify with iterator in reverse order
	iterator, err := lm.Iterator()
	assert.NoErrorf(err, "Error creating log iterator: %v", err)

	for i := recordCount - 1; i >= 0; i-- {
		assert.Truef(iterator.HasNext(), "Expected more records, but iterator has none")

		rec, err := iterator.Next()
		assert.NoErrorf(err, "Error getting next record from iterator: %v", err)

		assert.Equal(rec, records[i])
	}

	assert.Falsef(iterator.HasNext(), "Expected no more records, but iterator has more")
}
