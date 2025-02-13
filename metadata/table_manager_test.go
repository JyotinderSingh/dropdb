package metadata

import (
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"github.com/JyotinderSingh/dropdb/types"
	"testing"

	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/table"
	"github.com/JyotinderSingh/dropdb/tx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestMetadata(blockSize int, t *testing.T) (*TableManager, *tx.Transaction, func()) {
	dbDir := t.TempDir()

	fm, err := file.NewManager(dbDir, blockSize)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 8)

	transaction := tx.NewTransaction(fm, lm, bm, concurrency.NewLockTable())

	tm, err := NewTableManager(true, transaction)
	require.NoError(t, err)

	cleanup := func() {
		err := transaction.Commit()
		if err != nil {
			t.Error(err)
		}
	}

	return tm, transaction, cleanup
}

func TestTableManager_CreateTable(t *testing.T) {
	tm, txn, cleanup := setupTestMetadata(400, t)
	defer cleanup()

	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddBoolField("active")

	// Create a user-defined table
	err := tm.CreateTable("test_table", schema, txn)
	require.NoError(t, err)

	// Verify the table catalog contains `table_catalog`, `field_catalog`, and `test_table`
	ts, err := table.NewTableScan(txn, "table_catalog", tm.TableCatalogLayout())
	require.NoError(t, err)
	defer ts.Close()

	err = ts.BeforeFirst()
	require.NoError(t, err)

	tableEntries := map[string]struct{}{"table_catalog": {}, "field_catalog": {}, "test_table": {}}
	for {
		found, err := ts.Next()
		require.NoError(t, err)
		if !found {
			break
		}

		name, err := ts.GetString("table_name")
		require.NoError(t, err)
		delete(tableEntries, name)

		slotSize, err := ts.GetInt("slot_size")
		require.NoError(t, err)
		assert.Greater(t, slotSize, 0)
	}
	assert.Empty(t, tableEntries, "Unexpected entries in table_catalog")

	// Verify the field catalog contains metadata for `test_table` and system catalogs
	ts, err = table.NewTableScan(txn, "field_catalog", tm.FieldCatalogLayout())
	require.NoError(t, err)
	defer ts.Close()

	err = ts.BeforeFirst()
	require.NoError(t, err)

	userTableFields := map[string]struct{}{"id": {}, "name": {}, "active": {}}
	systemTables := map[string]bool{"table_catalog": true, "field_catalog": true}

	for {
		found, err := ts.Next()
		require.NoError(t, err)
		if !found {
			break
		}

		tableName, err := ts.GetString("table_name")
		require.NoError(t, err)

		fieldName, err := ts.GetString("field_name")
		require.NoError(t, err)

		if tableName == "test_table" {
			delete(userTableFields, fieldName)
		} else {
			assert.Contains(t, systemTables, tableName, "Unexpected table name in field_catalog")
		}
	}

	assert.Empty(t, userTableFields, "Fields for test_table are missing in field_catalog")
}

func TestTableManager_CreateMultipleTables(t *testing.T) {
	tm, txn, cleanup := setupTestMetadata(400, t)
	defer cleanup()

	userSchema := record.NewSchema()
	userSchema.AddIntField("id")
	userSchema.AddStringField("name", 20)
	err := tm.CreateTable("users", userSchema, txn)
	require.NoError(t, err)

	orderSchema := record.NewSchema()
	orderSchema.AddIntField("order_id")
	orderSchema.AddDateField("order_date")
	err = tm.CreateTable("orders", orderSchema, txn)
	require.NoError(t, err)

	ts, err := table.NewTableScan(txn, "table_catalog", tm.TableCatalogLayout())
	require.NoError(t, err)
	defer ts.Close()

	err = ts.BeforeFirst()
	require.NoError(t, err)

	tables := map[string]struct{}{"users": {}, "orders": {}}

	for {
		found, err := ts.Next()
		require.NoError(t, err)
		if !found {
			break
		}

		name, err := ts.GetString("table_name")
		require.NoError(t, err)
		delete(tables, name)
	}

	assert.Empty(t, tables)
}

func TestTableManager_GetLayout(t *testing.T) {
	tm, txn, cleanup := setupTestMetadata(400, t)
	defer cleanup()

	// Define and create a table schema
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddBoolField("active")

	tableName := "test_table"
	err := tm.CreateTable(tableName, schema, txn)
	require.NoError(t, err)

	// Retrieve the layout using GetLayout
	layout, err := tm.GetLayout(tableName, txn)
	require.NoError(t, err)

	// Validate that slot size is correctly retrieved
	expectedSlotSize := layout.SlotSize()
	tableCatalogScan, err := table.NewTableScan(txn, tableCatalogTable, tm.TableCatalogLayout())
	require.NoError(t, err)
	defer tableCatalogScan.Close()

	err = tableCatalogScan.BeforeFirst()
	require.NoError(t, err)

	foundTable := false
	for {
		hasNext, err := tableCatalogScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		tName, err := tableCatalogScan.GetString("table_name")
		require.NoError(t, err)

		if tName == tableName {
			foundTable = true
			slotSize, err := tableCatalogScan.GetInt("slot_size")
			require.NoError(t, err)
			assert.Equal(t, expectedSlotSize, slotSize, "Slot size mismatch")
		}
	}
	assert.True(t, foundTable, "Table not found in table_catalog")

	// Validate schema metadata in field catalog
	fieldCatalogScan, err := table.NewTableScan(txn, "field_catalog", tm.FieldCatalogLayout())
	require.NoError(t, err)
	defer fieldCatalogScan.Close()

	err = fieldCatalogScan.BeforeFirst()
	require.NoError(t, err)

	expectedFields := map[string]struct {
		fieldType types.SchemaType
		length    int
	}{
		"id":     {types.Integer, 0},
		"name":   {types.Varchar, 20},
		"active": {types.Boolean, 0},
	}

	for {
		hasNext, err := fieldCatalogScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		tName, err := fieldCatalogScan.GetString("table_name")
		require.NoError(t, err)

		if tName == tableName {
			fieldName, err := fieldCatalogScan.GetString("field_name")
			require.NoError(t, err)

			fieldType, err := fieldCatalogScan.GetInt("type")
			require.NoError(t, err)

			fieldLength, err := fieldCatalogScan.GetInt("length")
			require.NoError(t, err)

			assert.Contains(t, expectedFields, fieldName, "Unexpected field in field_catalog")

			expected := expectedFields[fieldName]
			assert.Equal(t, int(expected.fieldType), fieldType, "Field type mismatch")
			assert.Equal(t, expected.length, fieldLength, "Field length mismatch")
		}
	}
}
