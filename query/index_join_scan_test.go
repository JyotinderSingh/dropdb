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

type joinTestSetup struct {
	transaction *tx.Transaction
	lhsScan     *table.Scan
	rhsScan     *table.Scan
	idx         index.Index
	cleanup     func()
}

func setupJoinTest(t *testing.T, useHashIndex bool) *joinTestSetup {
	dbDir := t.TempDir()
	fm, err := file.NewManager(dbDir, 800)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 100)
	transaction := tx.NewTransaction(fm, lm, bm, concurrency.NewLockTable())

	// Create schemas and layouts for both tables
	lhsSchema := record.NewSchema()
	lhsSchema.AddIntField("id")
	lhsSchema.AddStringField("name", 20)
	lhsSchema.AddIntField("dept_id") // Join field
	lhsLayout := record.NewLayout(lhsSchema)

	rhsSchema := record.NewSchema()
	rhsSchema.AddIntField("dept_id") // Join field
	rhsSchema.AddStringField("dept_name", 20)
	rhsSchema.AddIntField("budget")
	rhsLayout := record.NewLayout(rhsSchema)

	// Create index schema and layout
	idxSchema := record.NewSchema()
	idxSchema.AddIntField(common.BlockField)
	idxSchema.AddIntField(common.IDField)
	idxSchema.AddIntField(common.DataValueField)
	idxLayout := record.NewLayout(idxSchema)

	// Create table scans
	lhs, err := table.NewTableScan(transaction, "employees", lhsLayout)
	require.NoError(t, err)

	rhs, err := table.NewTableScan(transaction, "departments", rhsLayout)
	require.NoError(t, err)

	// Create index
	var idx index.Index
	if useHashIndex {
		idx = hash.NewIndex(transaction, "dept_idx", idxLayout)
	} else {
		idx, err = btree.NewIndex(transaction, "dept_idx", idxLayout)
		require.NoError(t, err)
	}

	// Insert test data into employees (LHS)
	empData := []struct {
		id     int
		name   string
		deptID int
	}{
		{1, "Alice", 1}, // Marketing
		{2, "Bob", 2},   // Engineering
		{3, "Carol", 2}, // Engineering
		{4, "Dave", 3},  // Sales
		{5, "Eve", 1},   // Marketing
	}

	for _, d := range empData {
		require.NoError(t, lhs.Insert())
		require.NoError(t, lhs.SetInt("id", d.id))
		require.NoError(t, lhs.SetString("name", d.name))
		require.NoError(t, lhs.SetInt("dept_id", d.deptID))
	}

	// Insert test data into departments (RHS)
	deptData := []struct {
		deptID int
		name   string
		budget int
	}{
		{1, "Marketing", 100000},
		{2, "Engineering", 200000},
		{3, "Sales", 150000},
	}

	for _, d := range deptData {
		require.NoError(t, rhs.Insert())
		require.NoError(t, rhs.SetInt("dept_id", d.deptID))
		require.NoError(t, rhs.SetString("dept_name", d.name))
		require.NoError(t, rhs.SetInt("budget", d.budget))

		// Add to index
		rid := rhs.GetRecordID()
		require.NoError(t, idx.Insert(d.deptID, rid))
	}

	cleanup := func() {
		lhs.Close()
		rhs.Close()
		idx.Close()
		if err := transaction.Commit(); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(dbDir); err != nil {
			t.Error(err)
		}
	}

	return &joinTestSetup{
		transaction: transaction,
		lhsScan:     lhs,
		rhsScan:     rhs,
		idx:         idx,
		cleanup:     cleanup,
	}
}

func TestIndexJoinScan_Basic(t *testing.T) {
	tests := []struct {
		name         string
		useHashIndex bool
	}{
		{"HashIndex", true},
		{"BTreeIndex", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup := setupJoinTest(t, tt.useHashIndex)
			defer setup.cleanup()

			// Create index join scan
			ijs, err := NewIndexJoinScan(setup.lhsScan, setup.rhsScan, "dept_id", setup.idx)
			require.NoError(t, err)
			defer ijs.Close()

			// Track matches to verify all expected joins are found
			matches := make(map[string]bool)

			for {
				hasNext, err := ijs.Next()
				require.NoError(t, err)
				if !hasNext {
					break
				}

				empName, err := ijs.GetString("name")
				require.NoError(t, err)

				deptName, err := ijs.GetString("dept_name")
				require.NoError(t, err)

				budget, err := ijs.GetInt("budget")
				require.NoError(t, err)

				// Create a key to track unique combinations
				key := empName + "-" + deptName
				matches[key] = true

				// Verify joined record has correct budget based on department
				switch deptName {
				case "Marketing":
					assert.Equal(t, 100000, budget)
				case "Engineering":
					assert.Equal(t, 200000, budget)
				case "Sales":
					assert.Equal(t, 150000, budget)
				}
			}

			// Verify all expected joins were found
			expectedJoins := []string{
				"Alice-Marketing",
				"Bob-Engineering",
				"Carol-Engineering",
				"Dave-Sales",
				"Eve-Marketing",
			}

			assert.Equal(t, len(expectedJoins), len(matches))
			for _, join := range expectedJoins {
				assert.True(t, matches[join], "Expected join not found: %s", join)
			}
		})
	}
}

func TestIndexJoinScan_NoMatches(t *testing.T) {
	setup := setupJoinTest(t, true) // Using hash index for this test
	defer setup.cleanup()

	// Create a new department table and index containing just one department
	rhsSchema := record.NewSchema()
	rhsSchema.AddIntField("dept_id")
	rhsSchema.AddStringField("dept_name", 20)
	rhsSchema.AddIntField("budget")
	rhsLayout := record.NewLayout(rhsSchema)

	idxSchema := record.NewSchema()
	idxSchema.AddIntField(common.BlockField)
	idxSchema.AddIntField(common.IDField)
	idxSchema.AddIntField(common.DataValueField)
	idxLayout := record.NewLayout(idxSchema)

	rhs, err := table.NewTableScan(setup.transaction, "departments_nomatch", rhsLayout)
	require.NoError(t, err)
	defer rhs.Close()

	idx := hash.NewIndex(setup.transaction, "dept_idx_nomatch", idxLayout)
	defer idx.Close()

	// Insert only one department with ID 1
	require.NoError(t, rhs.Insert())
	require.NoError(t, rhs.SetInt("dept_id", 1))
	require.NoError(t, rhs.SetString("dept_name", "Existing"))
	require.NoError(t, rhs.SetInt("budget", 100000))
	rid := rhs.GetRecordID()
	require.NoError(t, idx.Insert(1, rid))

	// Create a new employee table with just one employee
	lhsSchema := record.NewSchema()
	lhsSchema.AddIntField("id")
	lhsSchema.AddStringField("name", 20)
	lhsSchema.AddIntField("dept_id")
	lhsLayout := record.NewLayout(lhsSchema)

	lhs, err := table.NewTableScan(setup.transaction, "employees_nomatch", lhsLayout)
	require.NoError(t, err)
	defer lhs.Close()

	// Insert one employee with non-existent department
	require.NoError(t, lhs.Insert())
	require.NoError(t, lhs.SetInt("id", 99))
	require.NoError(t, lhs.SetString("name", "NoMatch"))
	require.NoError(t, lhs.SetInt("dept_id", 999)) // Non-existent department

	// Reset scan position
	require.NoError(t, lhs.BeforeFirst())
	hasNext, err := lhs.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	// Create index join scan
	ijs, err := NewIndexJoinScan(lhs, rhs, "dept_id", idx)
	require.NoError(t, err)
	defer ijs.Close()

	// Should find no matches since dept_id=999 doesn't exist in departments
	hasNext, err = ijs.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

func TestIndexJoinScan_MultipleMatches(t *testing.T) {
	setup := setupJoinTest(t, false) // Using btree index for this test
	defer setup.cleanup()

	// Count employees in the Engineering department (should be 2)
	ijs, err := NewIndexJoinScan(setup.lhsScan, setup.rhsScan, "dept_id", setup.idx)
	require.NoError(t, err)
	defer ijs.Close()

	engineeringCount := 0
	for {
		hasNext, err := ijs.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		deptName, err := ijs.GetString("dept_name")
		require.NoError(t, err)
		if deptName == "Engineering" {
			engineeringCount++
		}
	}

	assert.Equal(t, 2, engineeringCount)
}

func TestIndexJoinScan_FieldAccess(t *testing.T) {
	setup := setupJoinTest(t, true) // Using hash index for this test
	defer setup.cleanup()

	ijs, err := NewIndexJoinScan(setup.lhsScan, setup.rhsScan, "dept_id", setup.idx)
	require.NoError(t, err)
	defer ijs.Close()

	// Move to first record
	hasNext, err := ijs.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	// Test HasField for fields from both tables
	assert.True(t, ijs.HasField("id"))        // LHS field
	assert.True(t, ijs.HasField("name"))      // LHS field
	assert.True(t, ijs.HasField("dept_id"))   // Both tables
	assert.True(t, ijs.HasField("dept_name")) // RHS field
	assert.True(t, ijs.HasField("budget"))    // RHS field
	assert.False(t, ijs.HasField("nonexistent"))

	// Test GetVal for fields from both tables
	id, err := ijs.GetInt("id")
	require.NoError(t, err)
	assert.Greater(t, id, 0)

	budget, err := ijs.GetInt("budget")
	require.NoError(t, err)
	assert.Greater(t, budget, 0)
}

func TestIndexJoinScan_BeforeFirst(t *testing.T) {
	setup := setupJoinTest(t, false) // Using btree index for this test
	defer setup.cleanup()

	ijs, err := NewIndexJoinScan(setup.lhsScan, setup.rhsScan, "dept_id", setup.idx)
	require.NoError(t, err)
	defer ijs.Close()

	// Count all joined records
	matchCount := 0
	for {
		hasNext, err := ijs.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		matchCount++
	}

	// Reset and count again
	require.NoError(t, ijs.BeforeFirst())
	secondCount := 0
	for {
		hasNext, err := ijs.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		secondCount++
	}

	// Both counts should be equal
	assert.Equal(t, matchCount, secondCount)
	assert.Equal(t, 5, matchCount) // Based on our test data
}
