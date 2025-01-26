package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestSortPlan_SingleField tests sorting by a single field
func TestSortPlan_SingleField(t *testing.T) {
	// 1) Setup environment
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	// 2) Create table and insert test data
	mdm := createTableMetadataWithSchema(t, txn, "employees", map[string]interface{}{
		"id":     0,
		"name":   "string",
		"salary": 0,
	})

	// Create a TablePlan for insertion
	tp, err := NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	// Insert records in unsorted order
	testRecords := []map[string]interface{}{
		{"id": 3, "name": "Carol", "salary": 60000},
		{"id": 1, "name": "Alice", "salary": 50000},
		{"id": 4, "name": "David", "salary": 45000},
		{"id": 2, "name": "Bob", "salary": 55000},
	}
	insertRecords(t, us, testRecords)

	// Re-instantiate TablePlan
	tp, err = NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	// 3) Create SortPlan sorting by salary
	sortPlan := NewSortPlan(txn, tp, []string{"salary"})

	// 4) Open the plan and verify records come out in sorted order
	sortScan, err := sortPlan.Open()
	require.NoError(t, err)
	defer sortScan.Close()

	var salaries []int
	require.NoError(t, sortScan.BeforeFirst())
	for {
		hasNext, err := sortScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		salary, err := sortScan.GetInt("salary")
		require.NoError(t, err)
		salaries = append(salaries, salary)
	}

	// Verify salaries are in ascending order
	assert.Equal(t, []int{45000, 50000, 55000, 60000}, salaries)

	// 5) Verify plan statistics
	assert.Equal(t, tp.BlocksAccessed(), sortPlan.BlocksAccessed())
	assert.Equal(t, tp.RecordsOutput(), sortPlan.RecordsOutput())
	assert.Equal(t, tp.DistinctValues("salary"), sortPlan.DistinctValues("salary"))
}

// TestSortPlan_MultipleFields tests sorting by multiple fields
func TestSortPlan_MultipleFields(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "employees", map[string]interface{}{
		"id":     0,
		"dept":   "string",
		"salary": 0,
		"name":   "string",
	})

	tp, err := NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	// Insert records with same dept but different salaries
	testRecords := []map[string]interface{}{
		{"id": 1, "dept": "Sales", "salary": 50000, "name": "Alice"},
		{"id": 2, "dept": "IT", "salary": 60000, "name": "Bob"},
		{"id": 3, "dept": "Sales", "salary": 45000, "name": "Carol"},
		{"id": 4, "dept": "IT", "salary": 60000, "name": "David"},
	}
	insertRecords(t, us, testRecords)

	tp, err = NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	// Sort by dept ASC, salary DESC
	sortPlan := NewSortPlan(txn, tp, []string{"dept", "salary"})

	sortScan, err := sortPlan.Open()
	require.NoError(t, err)
	defer sortScan.Close()

	var results []struct {
		dept   string
		salary int
	}

	require.NoError(t, sortScan.BeforeFirst())
	for {
		hasNext, err := sortScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		dept, err := sortScan.GetString("dept")
		require.NoError(t, err)
		salary, err := sortScan.GetInt("salary")
		require.NoError(t, err)

		results = append(results, struct {
			dept   string
			salary int
		}{dept, salary})
	}

	// Verify sort order: first by dept, then by salary
	expected := []struct {
		dept   string
		salary int
	}{
		{"IT", 60000},    // Bob
		{"IT", 60000},    // David
		{"Sales", 45000}, // Alice
		{"Sales", 50000}, // Carol
	}
	assert.Equal(t, expected, results)
}

// TestSortPlan_LargeDataset tests sorting with a larger dataset to ensure
// multiple runs are created and merged correctly
func TestSortPlan_LargeDataset(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 64) // Small buffer size to force multiple runs
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "numbers", map[string]interface{}{
		"val": 0,
	})

	tp, err := NewTablePlan(txn, "numbers", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	// Insert numbers in reverse order
	const numRecords = 100
	for i := numRecords; i > 0; i-- {
		require.NoError(t, us.Insert())
		require.NoError(t, us.SetInt("val", i))
	}

	tp, err = NewTablePlan(txn, "numbers", mdm)
	require.NoError(t, err)

	sortPlan := NewSortPlan(txn, tp, []string{"val"})

	sortScan, err := sortPlan.Open()
	require.NoError(t, err)
	defer sortScan.Close()

	// Verify records come out in sorted order
	var prev *int
	count := 0
	require.NoError(t, sortScan.BeforeFirst())
	for {
		hasNext, err := sortScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		val, err := sortScan.GetInt("val")
		require.NoError(t, err)

		if prev != nil {
			assert.LessOrEqual(t, *prev, val, "Values should be in ascending order")
		}
		prev = &val
		count++
	}

	assert.Equal(t, numRecords, count, "Should have all records after sorting")
}

// TestSortPlan_EmptyTable tests sorting an empty table
func TestSortPlan_EmptyTable(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "empty", map[string]interface{}{
		"id": 0,
	})

	tp, err := NewTablePlan(txn, "empty", mdm)
	require.NoError(t, err)

	sortPlan := NewSortPlan(txn, tp, []string{"id"})

	sortScan, err := sortPlan.Open()
	require.NoError(t, err)
	assert.Nil(t, sortScan)
}
