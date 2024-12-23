package metadata

import (
	"testing"

	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tablescan"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupStatMgr initializes a StatMgr for testing.
func setupStatMgr(t *testing.T, refreshLimit int) (*StatMgr, *TableManager, *tx.Transaction, func()) {
	tm, txn, cleanup := setupTestMetadata(400, t)
	statMgr, err := NewStatMgr(tm, txn, refreshLimit)
	require.NoError(t, err)
	return statMgr, tm, txn, cleanup
}

func TestStatMgr_GetStatInfo(t *testing.T) {
	statMgr, tableManager, txn, cleanup := setupStatMgr(t, 100)
	defer cleanup()

	// Create a schema and a table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	err := tableManager.CreateTable("test_table", schema, txn)
	require.NoError(t, err)

	// Insert some data
	layout, err := tableManager.GetLayout("test_table", txn)
	require.NoError(t, err)
	ts, err := tablescan.NewTableScan(txn, "test_table", layout)
	require.NoError(t, err)
	defer ts.Close()

	for i := 1; i <= 10; i++ {
		require.NoError(t, ts.Insert())
		require.NoError(t, ts.SetInt("id", i))
		require.NoError(t, ts.SetString("name", "name"+string(rune(i))))
	}

	// Retrieve statistics
	stats, err := statMgr.GetStatInfo("test_table", layout, txn)
	require.NoError(t, err)

	// Validate statistics
	assert.Equal(t, 10, stats.RecordsOutput(), "Number of records mismatch")
	assert.Equal(t, 4, stats.BlocksAccessed(), "Number of blocks mismatch")
	assert.Equal(t, 10, stats.DistinctValues("id"), "Distinct values for 'id' mismatch")
	assert.Equal(t, 10, stats.DistinctValues("name"), "Distinct values for 'name' mismatch")
}

func TestStatMgr_RefreshStatistics(t *testing.T) {
	statMgr, tableManager, txn, cleanup := setupStatMgr(t, 2)
	defer cleanup()

	// Create a schema and a table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	err := tableManager.CreateTable("test_table", schema, txn)
	require.NoError(t, err)

	// Insert some data
	layout, err := tableManager.GetLayout("test_table", txn)
	require.NoError(t, err)
	ts, err := tablescan.NewTableScan(txn, "test_table", layout)
	require.NoError(t, err)
	defer ts.Close()

	for i := 1; i <= 5; i++ {
		require.NoError(t, ts.Insert())
		require.NoError(t, ts.SetInt("id", i))
		require.NoError(t, ts.SetString("name", "name"+string(rune(i))))
	}

	// Call GetStatInfo twice to trigger a refresh
	for i := 0; i < 3; i++ {
		_, err := statMgr.GetStatInfo("test_table", layout, txn)
		require.NoError(t, err)
	}

	// Confirm that statistics are refreshed
	stats, err := statMgr.GetStatInfo("test_table", layout, txn)
	require.NoError(t, err)
	assert.Equal(t, 5, stats.RecordsOutput(), "Number of records mismatch after refresh")
}
