package hash

import (
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"os"
	"testing"

	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupHashIndexTest(t *testing.T) (*Index, *tx.Transaction, func()) {

	dbDir := t.TempDir()

	fm, err := file.NewManager(dbDir, 400)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 8)

	transaction := tx.NewTransaction(fm, lm, bm, concurrency.NewLockTable())

	schema := record.NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	schema.AddStringField("data_value", 20)

	layout := record.NewLayout(schema)
	indexName := "test_index"
	hashIndex := NewIndex(transaction, indexName, layout)

	cleanup := func() {
		hashIndex.Close()
		if err := transaction.Commit(); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(dbDir); err != nil {
			t.Error(err)
		}
	}

	return hashIndex, transaction, cleanup
}

func TestHashIndex_BeforeFirst(t *testing.T) {
	hashIndex, _, cleanup := setupHashIndexTest(t)
	defer cleanup()

	err := hashIndex.BeforeFirst("test_key")
	require.NoError(t, err)
	assert.True(t, hashIndex.tableScan != nil)
}

func TestHashIndex_Next(t *testing.T) {
	hashIndex, _, cleanup := setupHashIndexTest(t)
	defer cleanup()

	testRecord := record.NewID(1, 1)

	// Insert a record
	err := hashIndex.Insert("test_key", testRecord)
	require.NoError(t, err)

	// Set cursor before first record
	err = hashIndex.BeforeFirst("test_key")
	require.NoError(t, err)

	// Iterate through records
	hasNext, err := hashIndex.Next()
	require.NoError(t, err)
	assert.True(t, hasNext)

	// Validate the record being pointed to
	dataRecordID, err := hashIndex.GetDataRecordID()
	require.NoError(t, err)
	assert.Equal(t, testRecord, dataRecordID)

	currentValue, err := hashIndex.tableScan.GetString("data_value")
	require.NoError(t, err)
	assert.Equal(t, "test_key", currentValue)

	// No more records
	hasNext, err = hashIndex.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

func TestHashIndex_GetDataRecordID(t *testing.T) {
	hashIndex, _, cleanup := setupHashIndexTest(t)
	defer cleanup()

	// Insert a record
	dataRecordID := record.NewID(1, 1)
	err := hashIndex.Insert("test_key", dataRecordID)
	require.NoError(t, err)

	err = hashIndex.BeforeFirst("test_key")
	require.NoError(t, err)

	_, err = hashIndex.Next()
	require.NoError(t, err)

	id, err := hashIndex.GetDataRecordID()
	require.NoError(t, err)
	assert.Equal(t, dataRecordID, id)
}

func TestHashIndex_Insert(t *testing.T) {
	hashIndex, _, cleanup := setupHashIndexTest(t)
	defer cleanup()

	// Insert a record
	dataRecordID := record.NewID(1, 1)
	err := hashIndex.Insert("test_key", dataRecordID)
	require.NoError(t, err)

	// Verify insertion
	err = hashIndex.BeforeFirst("test_key")
	require.NoError(t, err)

	hasNext, err := hashIndex.Next()
	require.NoError(t, err)
	assert.True(t, hasNext)

	id, err := hashIndex.GetDataRecordID()
	require.NoError(t, err)
	assert.Equal(t, dataRecordID, id)

	currentValue, err := hashIndex.tableScan.GetString("data_value")
	require.NoError(t, err)
	assert.Equal(t, "test_key", currentValue)
}

func TestHashIndex_Delete(t *testing.T) {
	hashIndex, _, cleanup := setupHashIndexTest(t)
	defer cleanup()

	// Insert and then delete a record
	dataRecordID := record.NewID(1, 1)
	err := hashIndex.Insert("test_key", dataRecordID)
	require.NoError(t, err)

	err = hashIndex.Delete("test_key", dataRecordID)
	require.NoError(t, err)

	// Verify deletion
	err = hashIndex.BeforeFirst("test_key")
	require.NoError(t, err)

	hasNext, err := hashIndex.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

func TestHashIndex_Close(t *testing.T) {
	hashIndex, _, cleanup := setupHashIndexTest(t)
	defer cleanup()

	err := hashIndex.BeforeFirst("test_key")
	require.NoError(t, err)

	hashIndex.Close()

	// Verify that the table scan is closed
	assert.Nil(t, hashIndex.tableScan)
}

func TestHashIndex_SearchCost(t *testing.T) {
	numBlocks := 1000
	recordsPerBucket := 10

	cost := SearchCost(numBlocks, recordsPerBucket)
	expectedCost := numBlocks / numBuckets
	assert.Equal(t, expectedCost, cost)
}
