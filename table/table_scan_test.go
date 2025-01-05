package table

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestTable(t *testing.T) (*TableScan, *tx.Transaction, func()) {
	// temporary db directory
	dbDir := t.TempDir()

	// Set up temporary file manager
	fm, err := file.NewManager(dbDir, 400)
	require.NoError(t, err)

	// Set up log manager
	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	// Set up buffer manager
	bm := buffer.NewManager(fm, lm, 3) // small buffer size to test block overflow

	lt := concurrency.NewLockTable()

	// Create transaction
	transaction := tx.NewTransaction(fm, lm, bm, lt)

	// Create schema
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddBoolField("active")
	schema.AddDateField("created")
	schema.AddLongField("count")
	schema.AddShortField("code")

	// Create layout
	layout := record.NewLayout(schema)

	// Create table scan
	ts, err := NewTableScan(transaction, "test_table", layout)
	require.NoError(t, err)

	cleanup := func() {
		ts.Close()
		if err := transaction.Commit(); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(dbDir); err != nil {
			t.Error(err)
		}
	}

	return ts, transaction, cleanup
}

func TestTableScan_InsertAndRetrieve(t *testing.T) {
	ts, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Test data
	now := time.Now().Truncate(time.Second) // truncate for comparison

	// Insert record
	err := ts.Insert()
	require.NoError(t, err)

	err = ts.SetInt("id", 1)
	require.NoError(t, err)
	err = ts.SetString("name", "John")
	require.NoError(t, err)
	err = ts.SetBool("active", true)
	require.NoError(t, err)
	err = ts.SetDate("created", now)
	require.NoError(t, err)
	err = ts.SetLong("count", 1000)
	require.NoError(t, err)
	err = ts.SetShort("code", 42)
	require.NoError(t, err)

	// Move to beginning and read
	err = ts.BeforeFirst()
	require.NoError(t, err)

	found, err := ts.Next()
	require.NoError(t, err)
	require.True(t, found)

	// Verify values
	id, err := ts.GetInt("id")
	require.NoError(t, err)
	assert.Equal(t, 1, id)

	name, err := ts.GetString("name")
	require.NoError(t, err)
	assert.Equal(t, "John", name)

	active, err := ts.GetBool("active")
	require.NoError(t, err)
	assert.True(t, active)

	created, err := ts.GetDate("created")
	require.NoError(t, err)
	assert.Equal(t, now, created)

	count, err := ts.GetLong("count")
	require.NoError(t, err)
	assert.Equal(t, int64(1000), count)

	code, err := ts.GetShort("code")
	require.NoError(t, err)
	assert.Equal(t, int16(42), code)
}

func TestTableScan_MultipleRecords(t *testing.T) {
	ts, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert multiple records
	expectedIDs := []int{1, 2, 3, 4, 5}
	expectedNames := []string{"John", "Jane", "Bob", "Alice", "Charlie"}

	for i := range expectedIDs {
		err := ts.Insert()
		require.NoError(t, err)

		err = ts.SetInt("id", expectedIDs[i])
		require.NoError(t, err)
		err = ts.SetString("name", expectedNames[i])
		require.NoError(t, err)
	}

	// Read back and verify
	err := ts.BeforeFirst()
	require.NoError(t, err)

	var foundIDs []int
	var foundNames []string

	for {
		found, err := ts.Next()
		require.NoError(t, err)
		if !found {
			break
		}

		id, err := ts.GetInt("id")
		require.NoError(t, err)
		foundIDs = append(foundIDs, id)

		name, err := ts.GetString("name")
		require.NoError(t, err)
		foundNames = append(foundNames, name)
	}

	assert.Equal(t, expectedIDs, foundIDs)
	assert.Equal(t, expectedNames, foundNames)
}

func TestTableScan_Delete(t *testing.T) {
	ts, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert records
	err := ts.Insert()
	require.NoError(t, err)
	err = ts.SetInt("id", 1)
	require.NoError(t, err)

	err = ts.Insert()
	require.NoError(t, err)
	err = ts.SetInt("id", 2)
	require.NoError(t, err)

	// Delete first record
	err = ts.BeforeFirst()
	require.NoError(t, err)
	found, err := ts.Next()
	require.NoError(t, err)
	require.True(t, found)

	err = ts.Delete()
	require.NoError(t, err)

	// Verify only second record remains
	err = ts.BeforeFirst()
	require.NoError(t, err)

	found, err = ts.Next()
	require.NoError(t, err)
	require.True(t, found)

	id, err := ts.GetInt("id")
	require.NoError(t, err)
	assert.Equal(t, 2, id)

	found, err = ts.Next()
	require.NoError(t, err)
	assert.False(t, found)
}

func TestTableScan_RecordID(t *testing.T) {
	ts, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert a record
	err := ts.Insert()
	require.NoError(t, err)
	err = ts.SetInt("id", 1)
	require.NoError(t, err)
	err = ts.SetString("name", "John")
	require.NoError(t, err)

	// Get its RID
	rid := ts.GetRecordID()
	require.NotNil(t, rid)

	// Insert another record
	err = ts.Insert()
	require.NoError(t, err)
	err = ts.SetInt("id", 2)
	require.NoError(t, err)

	// Move back to first record using RID
	err = ts.MoveToRecordID(rid)
	require.NoError(t, err)

	// Verify it's the correct record
	id, err := ts.GetInt("id")
	require.NoError(t, err)
	assert.Equal(t, 1, id)

	name, err := ts.GetString("name")
	require.NoError(t, err)
	assert.Equal(t, "John", name)
}

func TestTableScan_MultiBlock(t *testing.T) {
	ts, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert many records to force multiple blocks
	numRecords := 100
	for i := 1; i <= numRecords; i++ {
		err := ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", i)
		require.NoError(t, err)
	}

	// Read back and verify
	err := ts.BeforeFirst()
	require.NoError(t, err)

	count := 0
	lastID := 0
	for {
		found, err := ts.Next()
		require.NoError(t, err)
		if !found {
			break
		}

		count++
		id, err := ts.GetInt("id")
		require.NoError(t, err)
		assert.Greater(t, id, lastID) // Verify records are in order
		lastID = id
	}

	assert.Equal(t, numRecords, count)
	assert.Equal(t, numRecords, lastID)
}

func TestTableScanOperations(t *testing.T) {
	dbDir := t.TempDir()
	defer os.RemoveAll(dbDir)

	// Set up managers
	fm, err := file.NewManager(dbDir, 400)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 8)
	lt := concurrency.NewLockTable()
	transaction := tx.NewTransaction(fm, lm, bm, lt)
	defer func(transaction *tx.Transaction) {
		err := transaction.Commit()
		if err != nil {
			t.Error(err)
		}
	}(transaction)

	// Create schema and layout
	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout := record.NewLayout(schema)

	// Verify field offsets
	assert.Equal(t, 8, layout.Offset("A"), "Incorrect offset for field A")
	assert.Equal(t, 16, layout.Offset("B"), "Incorrect offset for field B")

	// Create table scan
	ts, err := NewTableScan(transaction, "T", layout)
	require.NoError(t, err)
	defer ts.Close()

	// Use fixed seed for reproducible tests
	r := rand.New(rand.NewSource(42))

	// Insert 50 records with known values from fixed seed
	expectedRecords := make(map[int]string)
	var insertedValues []int
	for i := 0; i < 50; i++ {
		err := ts.Insert()
		require.NoError(t, err)

		n := r.Intn(51) // 0 to 50
		err = ts.SetInt("A", n)
		require.NoError(t, err)

		err = ts.SetString("B", fmt.Sprintf("rec%d", n))
		require.NoError(t, err)

		rid := ts.GetRecordID()
		require.NotNil(t, rid)
		expectedRecords[n] = fmt.Sprintf("rec%d", n)
		insertedValues = append(insertedValues, n)
	}

	// Verify we inserted 50 records
	assert.Equal(t, 50, len(insertedValues), "Should have inserted 50 records")

	// Delete records with A-values less than 25
	err = ts.BeforeFirst()
	require.NoError(t, err)

	deletedCount := 0
	var remainingValues []int

	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		a, err := ts.GetInt("A")
		require.NoError(t, err)

		b, err := ts.GetString("B")
		require.NoError(t, err)

		// Verify record format before deletion
		assert.Equal(t, fmt.Sprintf("rec%d", a), b, "Record format mismatch")

		if a < 25 {
			deletedCount++
			err = ts.Delete()
			require.NoError(t, err)
		} else {
			remainingValues = append(remainingValues, a)
		}
	}

	// Verify remaining records
	err = ts.BeforeFirst()
	require.NoError(t, err)

	var foundValues []int
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		a, err := ts.GetInt("A")
		require.NoError(t, err)

		// Verify each remaining value is >= 25
		assert.GreaterOrEqual(t, a, 25, "Found record with A < 25 after deletion")

		b, err := ts.GetString("B")
		require.NoError(t, err)

		// Verify record format is maintained
		assert.Equal(t, fmt.Sprintf("rec%d", a), b, "Record format mismatch after deletion")

		foundValues = append(foundValues, a)
	}

	// Verify we found all remaining records
	assert.Equal(t, len(remainingValues), len(foundValues), "Mismatch in number of remaining records")

	// Verify deletion count matches expected
	expectedDeleted := 0
	for _, v := range insertedValues {
		if v < 25 {
			expectedDeleted++
		}
	}
	assert.Equal(t, expectedDeleted, deletedCount, "Incorrect number of records deleted")
}
