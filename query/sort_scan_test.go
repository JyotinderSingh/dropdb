package query

import (
	"github.com/JyotinderSingh/dropdb/materialize"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// setupTempTable creates a temporary table and fills it with test data
func setupTempTable(t *testing.T, transaction *tx.Transaction, tableName string, data []struct {
	ID   int
	Name string
	Val  int
}) (*materialize.TempTable, error) {
	// Create schema
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("val")

	// Create temp table
	tempTable := materialize.NewTempTable(transaction, schema)

	// Open scan to insert data
	ts, err := tempTable.Open()
	require.NoError(t, err)
	defer ts.Close()

	// Insert data
	for _, row := range data {
		if err := ts.Insert(); err != nil {
			return nil, err
		}
		if err := ts.SetInt("id", row.ID); err != nil {
			return nil, err
		}
		if err := ts.SetString("name", row.Name); err != nil {
			return nil, err
		}
		if err := ts.SetInt("val", row.Val); err != nil {
			return nil, err
		}
	}

	return tempTable, nil
}

// TestSortScan_SingleRun tests sorting a single run of records
func TestSortScan_SingleRun(t *testing.T) {
	// Setup transaction and data
	transaction, _, cleanup := createTransactionAndLayout(t)
	defer cleanup()

	data := []struct {
		ID   int
		Name string
		Val  int
	}{
		{1, "Alice", 10},
		{2, "Bob", 20},
		{3, "Carol", 30},
		{4, "Dave", 40},
	}

	tempTable, err := setupTempTable(t, transaction, "sortscan_test_single", data)
	require.NoError(t, err)

	// Create runs slice with single run
	runs := []*materialize.TempTable{tempTable}

	// Create comparator that sorts by ID
	comparator := NewRecordComparator([]string{"id"})

	// Create sort scan
	ss, err := NewSortScan(runs, comparator)
	require.NoError(t, err)
	defer ss.Close()

	// Test scanning sorted records
	var sortedIDs []int
	require.NoError(t, ss.BeforeFirst())
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := ss.GetInt("id")
		require.NoError(t, err)
		sortedIDs = append(sortedIDs, id)
	}

	// Verify records are sorted by ID
	assert.Equal(t, []int{1, 2, 3, 4}, sortedIDs)
}

// TestSortScan_TwoRuns tests merging and sorting two runs
func TestSortScan_TwoRuns(t *testing.T) {
	transaction, _, cleanup := createTransactionAndLayout(t)
	defer cleanup()

	// Create two pre-sorted runs
	run1Data := []struct {
		ID   int
		Name string
		Val  int
	}{
		{1, "Alice", 10},
		{3, "Carol", 30},
	}

	run2Data := []struct {
		ID   int
		Name string
		Val  int
	}{
		{2, "Bob", 20},
		{4, "Dave", 40},
	}

	// Setup both temp tables
	tempTable1, err := setupTempTable(t, transaction, "sortscan_test_run1", run1Data)
	require.NoError(t, err)

	tempTable2, err := setupTempTable(t, transaction, "sortscan_test_run2", run2Data)
	require.NoError(t, err)

	runs := []*materialize.TempTable{tempTable1, tempTable2}

	// Sort by ID
	comparator := NewRecordComparator([]string{"id"})

	ss, err := NewSortScan(runs, comparator)
	require.NoError(t, err)
	defer ss.Close()

	// Collect sorted results
	var sortedIDs []int
	require.NoError(t, ss.BeforeFirst())
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := ss.GetInt("id")
		require.NoError(t, err)
		sortedIDs = append(sortedIDs, id)
	}

	// Verify merged sort results
	assert.Equal(t, []int{1, 2, 3, 4}, sortedIDs)
}

// TestSortScan_MultipleFields tests sorting by multiple fields
func TestSortScan_MultipleFields(t *testing.T) {
	transaction, _, cleanup := createTransactionAndLayout(t)
	defer cleanup()

	// Create two runs with data pre-sorted by val, then name
	run1Data := []struct {
		ID   int
		Name string
		Val  int
	}{
		{4, "Dave", 10},  // Lowest val in run1
		{1, "Alice", 30}, // Higher val
	}

	run2Data := []struct {
		ID   int
		Name string
		Val  int
	}{
		{2, "Bob", 20},   // Lowest val in run2
		{3, "Carol", 30}, // Higher val
	}
	
	tempTable1, err := setupTempTable(t, transaction, "sortscan_test_multi1", run1Data)
	require.NoError(t, err)

	tempTable2, err := setupTempTable(t, transaction, "sortscan_test_multi2", run2Data)
	require.NoError(t, err)

	runs := []*materialize.TempTable{tempTable1, tempTable2}

	// Sort by val (ascending) then name (ascending)
	comparator := NewRecordComparator([]string{"val", "name"})

	ss, err := NewSortScan(runs, comparator)
	require.NoError(t, err)
	defer ss.Close()

	// Collect sorted results
	var sortedPairs []struct {
		Name string
		Val  int
	}
	require.NoError(t, ss.BeforeFirst())
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := ss.GetString("name")
		require.NoError(t, err)
		val, err := ss.GetInt("val")
		require.NoError(t, err)
		sortedPairs = append(sortedPairs, struct {
			Name string
			Val  int
		}{name, val})
	}

	// Dave (10) -> Bob (20) -> Alice (30) -> Carol (30)
	expectedPairs := []struct {
		Name string
		Val  int
	}{
		{"Dave", 10},
		{"Bob", 20},
		{"Alice", 30},
		{"Carol", 30},
	}

	assert.Equal(t, expectedPairs, sortedPairs)
}

// TestSortScan_SaveRestorePosition tests saving and restoring scan position
func TestSortScan_SaveRestorePosition(t *testing.T) {
	transaction, _, cleanup := createTransactionAndLayout(t)
	defer cleanup()

	data := []struct {
		ID   int
		Name string
		Val  int
	}{
		{1, "Alice", 10},
		{2, "Bob", 20},
		{3, "Carol", 30},
		{4, "Dave", 40},
	}

	tempTable, err := setupTempTable(t, transaction, "sortscan_test_position", data)
	require.NoError(t, err)

	runs := []*materialize.TempTable{tempTable}
	comparator := NewRecordComparator([]string{"id"})

	ss, err := NewSortScan(runs, comparator)
	require.NoError(t, err)
	defer ss.Close()

	// Move to Bob's record (id=2)
	require.NoError(t, ss.BeforeFirst())
	hasNext, err := ss.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	hasNext, err = ss.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	// Save position at Bob's record
	ss.SavePosition()

	// Move forward to Dave
	hasNext, err = ss.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	hasNext, err = ss.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	name, err := ss.GetString("name")
	require.NoError(t, err)
	assert.Equal(t, "Dave", name)

	// Restore position to Bob
	require.NoError(t, ss.RestorePosition())

	name, err = ss.GetString("name")
	require.NoError(t, err)
	assert.Equal(t, "Bob", name)
}

// TestSortScan_EmptyRun tests behavior with an empty run
func TestSortScan_EmptyRun(t *testing.T) {
	transaction, _, cleanup := createTransactionAndLayout(t)
	defer cleanup()

	// Create empty temp table
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("val")

	tempTable := materialize.NewTempTable(transaction, schema)
	runs := []*materialize.TempTable{tempTable}

	comparator := NewRecordComparator([]string{"id"})

	ss, err := NewSortScan(runs, comparator)
	require.NoError(t, err)
	defer ss.Close()

	// Verify no records returned
	require.NoError(t, ss.BeforeFirst())
	hasNext, err := ss.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}
