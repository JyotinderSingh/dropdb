package metadata

import (
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/index"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func setupIndexInfoTest(t *testing.T) (*IndexInfo, *tx.Transaction, func()) {
	t.Helper()

	dbDir := t.TempDir()

	fm, err := file.NewManager(dbDir, 400)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 8)

	transaction := tx.NewTransaction(fm, lm, bm, concurrency.NewLockTable())

	tableSchema := record.NewSchema()
	tableSchema.AddIntField("block")
	tableSchema.AddIntField("id")
	tableSchema.AddStringField("data_value", 20)

	statInfo := NewStatInfo(10, 100, map[string]int{
		"block":      10,
		"id":         100,
		"data_value": 20,
	})

	indexInfo := NewIndexInfo(
		"test_index",
		"data_value",
		tableSchema,
		transaction,
		statInfo,
	)

	cleanup := func() {
		if err := transaction.Commit(); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(dbDir); err != nil {
			t.Error(err)
		}
	}

	return indexInfo, transaction, cleanup
}

func TestIndexInfo_InsertAndValidate(t *testing.T) {
	indexInfo, _, cleanup := setupIndexInfoTest(t)
	defer cleanup()

	idx := indexInfo.Open()

	// Insert records into the index
	err := idx.Insert("key1", record.NewID(1, 1))
	require.NoError(t, err)
	err = idx.Insert("key2", record.NewID(2, 2))
	require.NoError(t, err)
	err = idx.Insert("key1", record.NewID(3, 3)) // Duplicate key with different ID
	require.NoError(t, err)

	// Validate RecordsOutput and DistinctValues
	assert.Equal(t, 100/20, indexInfo.RecordsOutput(), "RecordsOutput mismatch") // numRecords / distinctValues
	assert.Equal(t, 1, indexInfo.DistinctValues("data_value"), "DistinctValues mismatch for indexed field")
	assert.Equal(t, 10, indexInfo.DistinctValues("block"), "DistinctValues mismatch for non-indexed field")
}

func TestIndexInfo_DeleteAndValidate(t *testing.T) {
	indexInfo, _, cleanup := setupIndexInfoTest(t)
	defer cleanup()

	idx := indexInfo.Open()

	// Insert and delete a record
	err := idx.Insert("key1", record.NewID(1, 1))
	require.NoError(t, err)
	err = idx.Delete("key1", record.NewID(1, 1))
	require.NoError(t, err)

	// Verify RecordsOutput and DistinctValues remain consistent
	assert.Equal(t, 100/20, indexInfo.RecordsOutput(), "RecordsOutput mismatch after deletion")
	assert.Equal(t, 1, indexInfo.DistinctValues("data_value"), "DistinctValues mismatch for indexed field after deletion")
}

func TestIndexInfo_CreateIndexLayout(t *testing.T) {
	indexInfo, _, cleanup := setupIndexInfoTest(t)
	defer cleanup()

	layout := indexInfo.CreateIndexLayout()
	require.NotNil(t, layout)

	schema := layout.Schema()
	assert.True(t, schema.HasField(index.BlockField))
	assert.True(t, schema.HasField(index.IDField))
	assert.True(t, schema.HasField(index.DataValueField))
	assert.Equal(t, record.Varchar, schema.Type(index.DataValueField))
}
