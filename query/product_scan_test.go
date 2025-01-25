package query

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/JyotinderSingh/dropdb/table"
)

// setupTestProductScan creates two small TableScans, inserts some sample rows,
// and returns a ProductScan over them, plus a cleanup function.
//
// Suppose the first table has columns "A" and "B".
// The second table has columns "X" and "Y".
// setupTestProductScan creates two small TableScans, inserts some sample rows,
// and returns a ProductScan over them, plus a cleanup function.
func setupTestProductScan(t *testing.T) (*ProductScan, func()) {
	// Create and populate the first table (A, B)
	ts1, cleanup1 := setupTestTable(t, "productscan_table1", func(schema *record.Schema) {
		schema.AddIntField("A")
		schema.AddStringField("B", 20)
	}, []map[string]interface{}{
		{"A": 1, "B": "alpha"},
		{"A": 2, "B": "beta"},
	})

	// Create and populate the second table (X, Y)
	ts2, cleanup2 := setupTestTable(t, "productscan_table2", func(schema *record.Schema) {
		schema.AddIntField("X")
		schema.AddStringField("Y", 20)
	}, []map[string]interface{}{
		{"X": 100, "Y": "foo"},
		{"X": 200, "Y": "bar"},
	})

	// Create the product scan
	ps := NewProductScan(ts1, ts2)

	// Combined cleanup function
	cleanup := func() {
		ps.Close()
		cleanup1()
		cleanup2()
	}

	return ps, cleanup
}

// setupTestTable sets up a TableScan with the given schema and data rows.
func setupTestTable(t *testing.T, tableName string, defineSchema func(*record.Schema), data []map[string]interface{}) (*table.Scan, func()) {
	// Create a transaction and layout
	tx, layout, cleanup := createCustomTransactionAndLayout(t, defineSchema)

	// Create the table scan
	ts, err := table.NewTableScan(tx, tableName, layout)
	require.NoError(t, err)

	// Insert the data rows
	for _, row := range data {
		require.NoError(t, ts.Insert())
		for field, value := range row {
			switch v := value.(type) {
			case int:
				require.NoError(t, ts.SetInt(field, v))
			case string:
				require.NoError(t, ts.SetString(field, v))
			default:
				t.Fatalf("unsupported field type for field %s: %T", field, v)
			}
		}
	}

	// Move to the beginning
	require.NoError(t, ts.BeforeFirst())

	// Return the TableScan and a combined cleanup function
	return ts, func() {
		ts.Close()
		cleanup()
	}
}

// createTransactionAndLayout creates a transaction and layout with the specified schema.
func createCustomTransactionAndLayout(t *testing.T, defineSchema func(*record.Schema)) (*tx.Transaction, *record.Layout, func()) {
	// Create a temporary directory for the test
	dbDir := t.TempDir()

	// Set up the database components
	fm, err := file.NewManager(dbDir, 800)
	require.NoError(t, err, "failed to create file manager")

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err, "failed to create log manager")

	bm := buffer.NewManager(fm, lm, 3)
	lt := concurrency.NewLockTable()
	transaction := tx.NewTransaction(fm, lm, bm, lt)

	// Define the schema
	schema := record.NewSchema()
	defineSchema(schema)

	// Create a layout from the schema
	layout := record.NewLayout(schema)

	// Define the cleanup function
	cleanup := func() {
		if err := transaction.Commit(); err != nil {
			t.Errorf("transaction commit failed: %v", err)
		}
		if err := os.RemoveAll(dbDir); err != nil {
			t.Errorf("failed to remove temp dir %s: %v", dbDir, err)
		}
	}

	return transaction, layout, cleanup
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

// TestProductScan_Basic tests that the product scan returns the cartesian product
// of two scans: (2 rows in table1) × (2 rows in table2) = 4 total rows.
func TestProductScan_Basic(t *testing.T) {
	ps, cleanup := setupTestProductScan(t)
	defer cleanup()

	// 2 rows in first table × 2 rows in second table = 4 total
	err := ps.BeforeFirst()
	require.NoError(t, err)

	var count int
	for {
		hasNext, err := ps.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
	}
	assert.Equal(t, 4, count, "ProductScan should produce 4 total rows from 2×2.")
}

// TestProductScan_FieldAccess verifies we can read fields from both sides.
func TestProductScan_FieldAccess(t *testing.T) {
	ps, cleanup := setupTestProductScan(t)
	defer cleanup()

	require.NoError(t, ps.BeforeFirst())

	// We'll gather the (A,B,X,Y) values from each row in the product.
	var rows []string
	for {
		hasNext, err := ps.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		// We expect to find "A" and "B" in scan1, "X" and "Y" in scan2.
		A, err := ps.GetInt("A")
		require.NoError(t, err)
		B, err := ps.GetString("B")
		require.NoError(t, err)
		X, err := ps.GetInt("X")
		require.NoError(t, err)
		Y, err := ps.GetString("Y")
		require.NoError(t, err)

		rows = append(rows, fmt.Sprintf("A=%d,B=%s,X=%d,Y=%s", A, B, X, Y))
	}

	// We expect 4 combined rows. Possible combos:
	//  (A=1,B=alpha) × (X=100,Y=foo)
	//  (A=1,B=alpha) × (X=200,Y=bar)
	//  (A=2,B=beta)  × (X=100,Y=foo)
	//  (A=2,B=beta)  × (X=200,Y=bar)
	assert.Len(t, rows, 4, "Should have 4 product rows")
	// Optionally check them in detail if you like:
	// e.g. "A=1,B=alpha,X=100,Y=foo", etc.
}

// TestProductScan_HasField verifies that HasField is true
// if the field is in scan1 or scan2.
func TestProductScan_HasField(t *testing.T) {
	ps, cleanup := setupTestProductScan(t)
	defer cleanup()

	// Our first scan has (A,B) and the second has (X,Y).
	assert.True(t, ps.HasField("A"))
	assert.True(t, ps.HasField("B"))
	assert.True(t, ps.HasField("X"))
	assert.True(t, ps.HasField("Y"))

	// Some random field not in either table
	assert.False(t, ps.HasField("Z"))
}

// TestProductScan_UpdateFields tries to update fields if underlying scans allow it.
// For example, let's assume table 1 is an UpdateScan for A,B, and table 2 is also an UpdateScan for X,Y.
// If they truly are, then `SetInt("A", ...)` will delegate to scan1, `SetInt("X", ...)` will delegate to scan2, etc.
func TestProductScan_UpdateFields(t *testing.T) {
	ps, cleanup := setupTestProductScan(t)
	defer cleanup()

	// If your underlying table scans are indeed UpdateScan, we can do something like:
	err := ps.BeforeFirst()
	require.NoError(t, err)

	updateCount := 0
	for {
		hasNext, err := ps.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		// Let's try to increment "A" by 10
		oldA, err := ps.GetInt("A")
		require.NoError(t, err)

		err = ps.SetInt("A", oldA+10)
		// If the underlying scan1 is an UpdateScan, this should succeed.
		// If not, your code will return "update not supported on scan: %T".
		if err == nil {
			updateCount++
		}

		// Similarly, try updating "X" by 100
		oldX, err := ps.GetInt("X")
		require.NoError(t, err)

		err = ps.SetInt("X", oldX+100)
		// Same note as above
		if err == nil {
			updateCount++
		}
	}

	// Possibly assert how many updates succeeded, depending on whether your table scans are updatable.
	t.Logf("Number of successful updates: %d", updateCount)
	// In many designs, a ProductScan is logically read-only, so you might get zero updates and errors.
	// Or if your underlying scans do support updates, you might see a nonzero count.
}

// TestProductScan_InsertDelete tests that insert/delete are not supported.
func TestProductScan_InsertDelete(t *testing.T) {
	ps, cleanup := setupTestProductScan(t)
	defer cleanup()

	err := ps.Insert()
	assert.Error(t, err, "Insert should fail on ProductScan")

	err = ps.Delete()
	assert.Error(t, err, "Delete should fail on ProductScan")
}

// TestProductScan_RecordID tests that record ID operations are not supported.
func TestProductScan_RecordID(t *testing.T) {
	ps, cleanup := setupTestProductScan(t)
	defer cleanup()

	// GetRecordID -> panic
	assert.Panics(t, func() {
		_ = ps.GetRecordID()
	}, "GetRecordID should panic on ProductScan")

	// MoveToRecordID -> error
	err := ps.MoveToRecordID(nil)
	assert.Error(t, err, "MoveToRecordID should fail on ProductScan")
}
