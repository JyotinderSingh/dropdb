package query

import (
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/index"
	"github.com/JyotinderSingh/dropdb/index/btree"
	"github.com/JyotinderSingh/dropdb/index/common"
	"github.com/JyotinderSingh/dropdb/index/hash"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/table"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

type testSetup struct {
	transaction *tx.Transaction
	tableScan   *table.TableScan
	idx         index.Index
	cleanup     func()
}

func setupTestWithIndex(t *testing.T, useHashIndex bool) *testSetup {
	dbDir := t.TempDir()
	fm, err := file.NewManager(dbDir, 800)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 100)
	transaction := tx.NewTransaction(fm, lm, bm, concurrency.NewLockTable())

	// Create table schema and layout
	tblSchema := record.NewSchema()
	tblSchema.AddIntField("id")
	tblSchema.AddStringField("name", 20)
	tblSchema.AddIntField("val")
	tblLayout := record.NewLayout(tblSchema)

	// Create index schema and layout
	idxSchema := record.NewSchema()
	idxSchema.AddIntField(common.BlockField)
	idxSchema.AddIntField(common.IDField)
	idxSchema.AddIntField(common.DataValueField)
	idxLayout := record.NewLayout(idxSchema)

	// Create table scan
	ts, err := table.NewTableScan(transaction, "test_table", tblLayout)
	require.NoError(t, err)

	// Create index
	var idx index.Index
	if useHashIndex {
		idx = hash.NewIndex(transaction, "test_idx", idxLayout)
	} else {
		idx, err = btree.NewIndex(transaction, "test_idx", idxLayout)
		require.NoError(t, err)
	}

	// Insert test data
	testData := []struct {
		id   int
		name string
		val  int
	}{
		{1, "Alice", 10},
		{2, "Bob", 20},
		{2, "Carol", 20}, // Duplicate val for testing
		{4, "Dave", 40},
	}

	for _, d := range testData {
		require.NoError(t, ts.Insert())
		require.NoError(t, ts.SetInt("id", d.id))
		require.NoError(t, ts.SetString("name", d.name))
		require.NoError(t, ts.SetInt("val", d.val))

		// Add to index
		rid := ts.GetRecordID()
		require.NoError(t, idx.Insert(d.val, rid))
	}

	cleanup := func() {
		ts.Close()
		idx.Close()
		if err := transaction.Commit(); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(dbDir); err != nil {
			t.Error(err)
		}
	}

	return &testSetup{
		transaction: transaction,
		tableScan:   ts,
		idx:         idx,
		cleanup:     cleanup,
	}
}

func TestIndexSelectScan_Basic(t *testing.T) {
	tests := []struct {
		name         string
		useHashIndex bool
	}{
		{"HashIndex", true},
		{"BTreeIndex", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := setupTestWithIndex(t, tt.useHashIndex)
			defer setup.cleanup()

			// Create index select scan for val=20
			iss, err := NewIndexSelectScan(setup.tableScan, setup.idx, 20)
			require.NoError(t, err)
			defer iss.Close()

			// Should find both Bob and Carol
			matchCount := 0
			require.NoError(t, iss.BeforeFirst())
			for {
				hasNext, err := iss.Next()
				require.NoError(t, err)
				if !hasNext {
					break
				}

				name, err := iss.GetString("name")
				require.NoError(t, err)
				assert.Contains(t, []string{"Bob", "Carol"}, name)

				val, err := iss.GetInt("val")
				require.NoError(t, err)
				assert.Equal(t, 20, val)

				matchCount++
			}
			assert.Equal(t, 2, matchCount)
		})
	}
}

func TestIndexSelectScan_NoMatches(t *testing.T) {
	tests := []struct {
		name         string
		useHashIndex bool
	}{
		{"HashIndex", true},
		{"BTreeIndex", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := setupTestWithIndex(t, tt.useHashIndex)
			defer setup.cleanup()

			iss, err := NewIndexSelectScan(setup.tableScan, setup.idx, 99) // Non-existent value
			require.NoError(t, err)
			defer iss.Close()

			hasNext, err := iss.Next()
			require.NoError(t, err)
			assert.False(t, hasNext)
		})
	}
}

func TestIndexSelectScan_SingleMatch(t *testing.T) {
	tests := []struct {
		name         string
		useHashIndex bool
	}{
		{"HashIndex", true},
		{"BTreeIndex", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := setupTestWithIndex(t, tt.useHashIndex)
			defer setup.cleanup()

			iss, err := NewIndexSelectScan(setup.tableScan, setup.idx, 40) // Only Dave has val=40
			require.NoError(t, err)
			defer iss.Close()

			hasNext, err := iss.Next()
			require.NoError(t, err)
			assert.True(t, hasNext)

			name, err := iss.GetString("name")
			require.NoError(t, err)
			assert.Equal(t, "Dave", name)

			// Should be no more matches
			hasNext, err = iss.Next()
			require.NoError(t, err)
			assert.False(t, hasNext)
		})
	}
}

func TestIndexSelectScan_AllFields(t *testing.T) {
	setup := setupTestWithIndex(t, true) // Using hash index for this test
	defer setup.cleanup()

	iss, err := NewIndexSelectScan(setup.tableScan, setup.idx, 20)
	require.NoError(t, err)
	defer iss.Close()

	hasNext, err := iss.Next()
	require.NoError(t, err)
	assert.True(t, hasNext)

	// Test all field access methods
	id, err := iss.GetInt("id")
	require.NoError(t, err)
	assert.Equal(t, 2, id)

	name, err := iss.GetString("name")
	require.NoError(t, err)
	assert.Contains(t, []string{"Bob", "Carol"}, name)

	val, err := iss.GetVal("val")
	require.NoError(t, err)
	assert.Equal(t, 20, val)

	// Test HasField
	assert.True(t, iss.HasField("id"))
	assert.True(t, iss.HasField("name"))
	assert.True(t, iss.HasField("val"))
	assert.False(t, iss.HasField("nonexistent"))
}

func TestIndexSelectScan_BeforeFirst(t *testing.T) {
	tests := []struct {
		name         string
		useHashIndex bool
	}{
		{"HashIndex", true},
		{"BTreeIndex", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := setupTestWithIndex(t, tt.useHashIndex)
			defer setup.cleanup()

			iss, err := NewIndexSelectScan(setup.tableScan, setup.idx, 20)
			require.NoError(t, err)
			defer iss.Close()

			// First iteration
			matchCount := 0
			require.NoError(t, iss.BeforeFirst())
			for {
				hasNext, err := iss.Next()
				require.NoError(t, err)
				if !hasNext {
					break
				}
				matchCount++
			}
			assert.Equal(t, 2, matchCount)

			// Second iteration after BeforeFirst
			matchCount = 0
			require.NoError(t, iss.BeforeFirst())
			for {
				hasNext, err := iss.Next()
				require.NoError(t, err)
				if !hasNext {
					break
				}
				matchCount++
			}
			assert.Equal(t, 2, matchCount)
		})
	}
}
