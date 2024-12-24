package metadata

import (
	"github.com/JyotinderSingh/dropdb/tablescan"
	"testing"

	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupIndexManagerTest(t *testing.T) (*TableManager, *IndexManager, *tx.Transaction, func()) {
	t.Helper()

	tm, txn, cleanup := setupTestMetadata(400, t)
	sm, err := NewStatManager(tm, txn, 100)
	require.NoError(t, err)
	indexManager, err := NewIndexManager(true, tm, sm, txn)
	require.NoError(t, err)
	return tm, indexManager, txn, cleanup
}

func TestIndexManager_CreateIndex(t *testing.T) {
	tm, indexManager, txn, cleanup := setupIndexManagerTest(t)
	defer cleanup()

	// Define schema and create a table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	err := tm.CreateTable("test_table", schema, txn)
	require.NoError(t, err)

	// Create an index on the "id" field
	err = indexManager.CreateIndex("test_index", "test_table", "id", txn)
	require.NoError(t, err)

	// Verify index metadata in index_catalog
	indexCatalogLayout := indexManager.layout
	ts, err := tablescan.NewTableScan(txn, indexCatalogTable, indexCatalogLayout)
	require.NoError(t, err)
	defer ts.Close()

	err = ts.BeforeFirst()
	require.NoError(t, err)

	found := false
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		indexName, err := ts.GetString(indexNameField)
		require.NoError(t, err)

		if indexName != "test_index" {
			continue
		}

		tableName, err := ts.GetString("table_name")
		require.NoError(t, err)
		assert.Equal(t, "test_table", tableName, "Table name mismatch in index_catalog")

		fieldName, err := ts.GetString("field_name")
		require.NoError(t, err)
		assert.Equal(t, "id", fieldName, "Field name mismatch in index_catalog")

		found = true
		break
	}

	assert.True(t, found, "Index not found in index_catalog")
}

func TestIndexManager_GetIndexInfo(t *testing.T) {
	tm, indexManager, txn, cleanup := setupIndexManagerTest(t)
	defer cleanup()

	// Define schema and create a table
	schema := record.NewSchema()
	schema.AddIntField("id")
	err := tm.CreateTable("test_table", schema, txn)
	require.NoError(t, err)

	// Create an index on the "id" field
	err = indexManager.CreateIndex("test_index", "test_table", "id", txn)
	require.NoError(t, err)

	// Retrieve index info
	indexInfos, err := indexManager.GetIndexInfo("test_table", txn)
	require.NoError(t, err)
	assert.Contains(t, indexInfos, "id", "IndexInfo for 'id' field not found")

	indexInfo := indexInfos["id"]
	assert.Equal(t, "test_index", indexInfo.indexName)
	assert.Equal(t, "id", indexInfo.fieldName)

	// Open the index and perform operations
	idx := indexInfo.Open()
	err = idx.Insert(1234, record.NewID(1, 1))
	require.NoError(t, err)
	err = idx.BeforeFirst(1234)
	require.NoError(t, err)
	hasNext, err := idx.Next()
	require.NoError(t, err)
	assert.True(t, hasNext)
}
