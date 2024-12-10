package tx

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/utils"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSetup creates a new test environment and returns cleanup function
func testSetup(t *testing.T) (*file.Manager, *log.Manager, func()) {
	testDir := filepath.Join("testdir", t.Name())
	fm, err := file.NewManager(testDir, 400)
	require.NoError(t, err, "Error initializing file manager")

	lm, err := log.NewManager(fm, "testlog")
	require.NoError(t, err, "Error initializing log manager")

	cleanup := func() {
		err := os.RemoveAll(testDir)
		if err != nil {
			t.Errorf("Failed to clean up test directory: %v", err)
		}
	}

	return fm, lm, cleanup
}

func TestSetBoolRecord(t *testing.T) {
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	block := file.NewBlockId("testfile", 1)
	page := file.NewPage(fm.BlockSize())

	txNum := 1
	offset := 100
	oldValue := false

	// Set page values
	page.SetInt(0, int(SetBool))
	page.SetInt(utils.IntSize, txNum)
	require.NoError(t, page.SetString(2*utils.IntSize, block.Filename()))
	page.SetInt(2*utils.IntSize+file.MaxLength(len(block.Filename())), block.Number())
	page.SetInt(3*utils.IntSize+file.MaxLength(len(block.Filename())), offset)
	page.SetBool(4*utils.IntSize+file.MaxLength(len(block.Filename())), oldValue)

	// Test record creation
	record, err := NewSetBoolRecord(page)
	require.NoError(t, err)
	assert.Equal(t, "<SETBOOL 1 [file testfile, block 1] 100 false>", record.String())

	// Test log writing
	lsn, err := WriteSetBoolToLog(lm, txNum, block, offset, oldValue)
	require.NoError(t, err)
	assert.True(t, lsn > 0)

	// Verify log content
	iter, err := lm.Iterator()
	require.NoError(t, err)
	require.True(t, iter.HasNext())

	bytes, err := iter.Next()
	require.NoError(t, err)

	logRecord, err := CreateLogRecord(bytes)
	require.NoError(t, err)
	assert.Equal(t, record.String(), logRecord.String())
}

func TestSetDateRecord(t *testing.T) {
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	block := file.NewBlockId("testfile", 1)
	page := file.NewPage(fm.BlockSize())

	txNum := 1
	offset := 200
	oldValue := time.Now().Truncate(time.Second) // Truncate for consistent comparison

	// Set page values
	page.SetInt(0, int(SetDate))
	page.SetInt(utils.IntSize, txNum)
	require.NoError(t, page.SetString(2*utils.IntSize, block.Filename()))
	page.SetInt(2*utils.IntSize+file.MaxLength(len(block.Filename())), block.Number())
	page.SetInt(3*utils.IntSize+file.MaxLength(len(block.Filename())), offset)
	page.SetDate(4*utils.IntSize+file.MaxLength(len(block.Filename())), oldValue)

	// Test record creation
	record, err := NewSetDateRecord(page)
	require.NoError(t, err)
	expectedStr := fmt.Sprintf("<SETDATE 1 [file testfile, block 1] 200 %s>",
		time.Unix(oldValue.Unix(), 0))
	assert.Equal(t, expectedStr, record.String())

	// Test log writing
	lsn, err := WriteSetDateToLog(lm, txNum, block, offset, oldValue)
	require.NoError(t, err)
	assert.True(t, lsn > 0)

	// Verify log content
	iter, err := lm.Iterator()
	require.NoError(t, err)
	require.True(t, iter.HasNext())

	bytes, err := iter.Next()
	require.NoError(t, err)

	logRecord, err := CreateLogRecord(bytes)
	require.NoError(t, err)
	assert.Equal(t, record.String(), logRecord.String())
}

func TestSetIntRecord(t *testing.T) {
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	block := file.NewBlockId("testfile", 1)
	page := file.NewPage(fm.BlockSize())

	txNum := 1
	offset := 300
	oldValue := 42

	// Set page values
	page.SetInt(0, int(SetInt))
	page.SetInt(utils.IntSize, txNum)
	require.NoError(t, page.SetString(2*utils.IntSize, block.Filename()))
	page.SetInt(2*utils.IntSize+file.MaxLength(len(block.Filename())), block.Number())
	page.SetInt(3*utils.IntSize+file.MaxLength(len(block.Filename())), offset)
	page.SetInt(4*utils.IntSize+file.MaxLength(len(block.Filename())), oldValue)

	// Test record creation
	record, err := NewSetIntRecord(page)
	require.NoError(t, err)
	assert.Equal(t, "<SETINT 1 [file testfile, block 1] 300 42>", record.String())

	// Test log writing
	lsn, err := WriteSetIntToLog(lm, txNum, block, offset, oldValue)
	require.NoError(t, err)
	assert.True(t, lsn > 0)

	// Verify log content
	iter, err := lm.Iterator()
	require.NoError(t, err)
	require.True(t, iter.HasNext())

	bytes, err := iter.Next()
	require.NoError(t, err)

	logRecord, err := CreateLogRecord(bytes)
	require.NoError(t, err)
	assert.Equal(t, record.String(), logRecord.String())
}

func TestSetLongRecord(t *testing.T) {
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	block := file.NewBlockId("testfile", 1)
	page := file.NewPage(fm.BlockSize())

	txNum := 1
	offset := 400
	oldValue := int64(987654321)

	// Set page values
	page.SetInt(0, int(SetLong))
	page.SetInt(utils.IntSize, txNum)
	require.NoError(t, page.SetString(2*utils.IntSize, block.Filename()))
	page.SetInt(2*utils.IntSize+file.MaxLength(len(block.Filename())), block.Number())
	page.SetInt(3*utils.IntSize+file.MaxLength(len(block.Filename())), offset)
	page.SetLong(4*utils.IntSize+file.MaxLength(len(block.Filename())), oldValue)

	// Test record creation
	record, err := NewSetLongRecord(page)
	require.NoError(t, err)
	assert.Equal(t, "<SETLONG 1 [file testfile, block 1] 400 987654321>", record.String())

	// Test log writing
	lsn, err := WriteSetLongToLog(lm, txNum, block, offset, oldValue)
	require.NoError(t, err)
	assert.True(t, lsn > 0)

	// Verify log content
	iter, err := lm.Iterator()
	require.NoError(t, err)
	require.True(t, iter.HasNext())

	bytes, err := iter.Next()
	require.NoError(t, err)

	logRecord, err := CreateLogRecord(bytes)
	require.NoError(t, err)
	assert.Equal(t, record.String(), logRecord.String())
}

func TestSetShortRecord(t *testing.T) {
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	block := file.NewBlockId("testfile", 1)
	page := file.NewPage(fm.BlockSize())

	txNum := 1
	offset := 500
	oldValue := int16(1234)

	// Set page values
	page.SetInt(0, int(SetShort))
	page.SetInt(utils.IntSize, txNum)
	require.NoError(t, page.SetString(2*utils.IntSize, block.Filename()))
	page.SetInt(2*utils.IntSize+file.MaxLength(len(block.Filename())), block.Number())
	page.SetInt(3*utils.IntSize+file.MaxLength(len(block.Filename())), offset)
	page.SetShort(4*utils.IntSize+file.MaxLength(len(block.Filename())), oldValue)

	// Test record creation
	record, err := NewSetShortRecord(page)
	require.NoError(t, err)
	assert.Equal(t, "<SETSHORT 1 [file testfile, block 1] 500 1234>", record.String())

	// Test log writing
	lsn, err := WriteSetShortToLog(lm, txNum, block, offset, oldValue)
	require.NoError(t, err)
	assert.True(t, lsn > 0)

	// Verify log content
	iter, err := lm.Iterator()
	require.NoError(t, err)
	require.True(t, iter.HasNext())

	bytes, err := iter.Next()
	require.NoError(t, err)

	logRecord, err := CreateLogRecord(bytes)
	require.NoError(t, err)
	assert.Equal(t, record.String(), logRecord.String())
}

func TestSetStringRecord(t *testing.T) {
	fm, lm, cleanup := testSetup(t)
	defer cleanup()

	block := file.NewBlockId("testfile", 1)
	page := file.NewPage(fm.BlockSize())

	txNum := 1
	offset := 600
	oldValue := "Hello, World!"

	// Set page values
	page.SetInt(0, int(SetString))
	page.SetInt(utils.IntSize, txNum)
	require.NoError(t, page.SetString(2*utils.IntSize, block.Filename()))
	page.SetInt(2*utils.IntSize+file.MaxLength(len(block.Filename())), block.Number())
	page.SetInt(3*utils.IntSize+file.MaxLength(len(block.Filename())), offset)
	require.NoError(t, page.SetString(4*utils.IntSize+file.MaxLength(len(block.Filename())), oldValue))

	// Test record creation
	record, err := NewSetStringRecord(page)
	require.NoError(t, err)
	assert.Equal(t, "<SETSTRING 1 [file testfile, block 1] 600 Hello, World!>", record.String())

	// Test log writing
	lsn, err := WriteSetStringToLog(lm, txNum, block, offset, oldValue)
	require.NoError(t, err)
	assert.True(t, lsn > 0)

	// Verify log content
	iter, err := lm.Iterator()
	require.NoError(t, err)
	require.True(t, iter.HasNext())

	bytes, err := iter.Next()
	require.NoError(t, err)

	logRecord, err := CreateLogRecord(bytes)
	require.NoError(t, err)
	assert.Equal(t, record.String(), logRecord.String())
}

// TestMultipleLogRecords tests writing and reading multiple different types of records
func TestMultipleLogRecords(t *testing.T) {
	_, lm, cleanup := testSetup(t)
	defer cleanup()

	block := file.NewBlockId("testfile", 1)
	txNum := 1

	// Write multiple records of different types
	type logWrite struct {
		write    func() (int, error)
		expected string
	}

	// Setup test data with write functions and expected output
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	writes := []logWrite{
		{
			write: func() (int, error) {
				return WriteSetBoolToLog(lm, txNum, block, 100, true)
			},
			expected: "<SETBOOL 1 [file testfile, block 1] 100 true>",
		},
		{
			write: func() (int, error) {
				return WriteSetDateToLog(lm, txNum, block, 200, testTime)
			},
			expected: fmt.Sprintf("<SETDATE 1 [file testfile, block 1] 200 %s>",
				time.Unix(testTime.Unix(), 0)),
		},
		{
			write: func() (int, error) {
				return WriteSetIntToLog(lm, txNum, block, 300, 42)
			},
			expected: "<SETINT 1 [file testfile, block 1] 300 42>",
		},
		{
			write: func() (int, error) {
				return WriteSetLongToLog(lm, txNum, block, 400, 987654321)
			},
			expected: "<SETLONG 1 [file testfile, block 1] 400 987654321>",
		},
		{
			write: func() (int, error) {
				return WriteSetShortToLog(lm, txNum, block, 500, 1234)
			},
			expected: "<SETSHORT 1 [file testfile, block 1] 500 1234>",
		},
		{
			write: func() (int, error) {
				return WriteSetStringToLog(lm, txNum, block, 600, "Test String")
			},
			expected: "<SETSTRING 1 [file testfile, block 1] 600 Test String>",
		},
	}

	// Write all records
	var lsns []int
	for _, w := range writes {
		lsn, err := w.write()
		require.NoError(t, err)
		require.True(t, lsn > 0)
		lsns = append(lsns, lsn)
	}

	// Verify records were written in order with ascending LSNs
	for i := 1; i < len(lsns); i++ {
		assert.Greater(t, lsns[i], lsns[i-1], "LSNs should be strictly increasing")
	}

	// Read and verify all records
	iter, err := lm.Iterator()
	require.NoError(t, err)

	recordCount := 0
	for iter.HasNext() {
		bytes, err := iter.Next()
		require.NoError(t, err)

		record, err := CreateLogRecord(bytes)
		require.NoError(t, err)

		require.Less(t, recordCount, len(writes),
			"Found more records than expected")

		idx := len(writes) - recordCount - 1 // Iterator reads log records in reverse order
		assert.Equal(t, writes[idx].expected, record.String(),
			"Record %d content mismatch", recordCount)
		recordCount++
	}

	assert.Equal(t, len(writes), recordCount,
		"Number of records read doesn't match number written")
}
