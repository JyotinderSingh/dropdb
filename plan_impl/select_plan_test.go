package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"

	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/scan"
)

// TestSelectPlan_BasicEquality tests a simple predicate (equality condition).
func TestSelectPlan_BasicEquality(t *testing.T) {
	// 1) Setup environment
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	// 2) Create table and insert test data
	mdm := createTableMetadataWithSchema(t, txn, "users", map[string]interface{}{
		"id":     0,
		"name":   "string",
		"active": true,
	})

	// Create a TablePlan so we can insert records
	tp, err := NewTablePlan(txn, "users", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	testRecords := []map[string]interface{}{
		{"id": 1, "name": "Alice", "active": true},
		{"id": 2, "name": "Bob", "active": false},
		{"id": 3, "name": "Carol", "active": true},
		{"id": 4, "name": "Dan", "active": false},
	}
	insertRecords(t, us, testRecords)

	// Re-instantiate the TablePlan so stats are recalculated
	tp, err = NewTablePlan(txn, "users", mdm)
	require.NoError(t, err)

	// 3) Build a simple predicate: active = true
	eqTerm := query.NewTerm(
		query.NewFieldExpression("active"), // LHS
		query.NewConstantExpression(true),  // RHS
		types.EQ,                           // Operator
	)
	pred := query.NewPredicateFromTerm(eqTerm)

	// 4) Create a SelectPlan wrapping the table plan
	sp := NewSelectPlan(tp, pred)

	// 5) Open the SelectPlan and verify only matching records are returned
	selScan, err := sp.Open()
	require.NoError(t, err)
	defer selScan.Close()

	require.NoError(t, selScan.BeforeFirst())

	count := 0
	for {
		hasNext, err := selScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++

		// active must be true for matching records
		activeVal, err := selScan.GetBool("active")
		require.NoError(t, err)
		assert.True(t, activeVal)
	}

	// We inserted 2 records with active = true
	assert.Equal(t, 2, count, "Should have 2 records with active = true")

	// 6) (Optional) Check plan-level statistics (BlocksAccessed, RecordsOutput, etc.)
	assert.GreaterOrEqual(t, sp.BlocksAccessed(), 1, "BlocksAccessed should be >= 1")

	estRecords := sp.RecordsOutput()
	// Because the stats-based reduction factor is heuristic, we just ensure it’s in a plausible range.
	assert.InDelta(t, 2, estRecords, 1, "RecordsOutput should be around 2, within a small delta")
}

// TestSelectPlan_Range tests a range predicate (e.g., id < 50).
func TestSelectPlan_Range(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "nums", map[string]interface{}{
		"id": 0,
	})

	tp, err := NewTablePlan(txn, "nums", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	// Insert 100 records with random id in [1..100]
	r := rand.New(rand.NewSource(123))
	for i := 0; i < 100; i++ {
		require.NoError(t, us.Insert())
		require.NoError(t, us.SetInt("id", r.Intn(100)+1))
	}

	// Re-instantiate TablePlan to refresh stats
	tp, err = NewTablePlan(txn, "nums", mdm)
	require.NoError(t, err)

	// Create predicate: id < 50
	ltTerm := query.NewTerm(
		query.NewFieldExpression("id"),  // LHS
		query.NewConstantExpression(50), // RHS
		types.LT,                        // Operator
	)
	pred := query.NewPredicateFromTerm(ltTerm)

	sp := NewSelectPlan(tp, pred)

	selScan, err := sp.Open()
	require.NoError(t, err)
	defer selScan.Close()

	require.NoError(t, selScan.BeforeFirst())
	countLessThan50 := 0

	for {
		hasNext, err := selScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		val, err := selScan.GetInt("id")
		require.NoError(t, err)
		// Check that val < 50
		require.True(t, val < 50)
		countLessThan50++
	}

	// We can't know exactly how many were < 50 because random,
	// but let's ensure at least one record matched.
	assert.Greater(t, countLessThan50, 0, "Should have at least 1 record with id < 50")

	// Check the plan-level estimates
	assert.Equal(t, tp.BlocksAccessed(), sp.BlocksAccessed(), "BlocksAccessed should match underlying plan")
	// For random distribution in [1..100], about half might be < 50:
	est := sp.RecordsOutput()
	assert.InDelta(t, 50, est, 10, "RecordsOutput should be around 50, within a small delta")
}

// TestSelectPlan_MultipleConditions tests an 'AND' predicate with two terms.
func TestSelectPlan_MultipleConditions(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "people", map[string]interface{}{
		"id":     0,
		"name":   "string",
		"active": true,
	})

	tp, err := NewTablePlan(txn, "people", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	// Insert some data
	insertRecords(t, us, []map[string]interface{}{
		{"id": 1, "name": "Alice", "active": true},
		{"id": 2, "name": "Bob", "active": false},
		{"id": 3, "name": "Alice", "active": false},
		{"id": 4, "name": "Bob", "active": true},
		{"id": 5, "name": "Charlie", "active": true},
	})

	// Re-instantiate TablePlan (to refresh stats if needed)
	tp, err = NewTablePlan(txn, "people", mdm)
	require.NoError(t, err)

	// Build predicate: (name = "Alice") AND (active = true)
	termNameEqAlice := query.NewTerm(
		query.NewFieldExpression("name"),
		query.NewConstantExpression("Alice"),
		types.EQ,
	)
	predNameEqAlice := query.NewPredicateFromTerm(termNameEqAlice)

	termActiveEqTrue := query.NewTerm(
		query.NewFieldExpression("active"),
		query.NewConstantExpression(true),
		types.EQ,
	)
	predActiveEqTrue := query.NewPredicateFromTerm(termActiveEqTrue)

	// Conjoin the two single-term predicates
	predNameEqAlice.ConjoinWith(predActiveEqTrue)

	sp := NewSelectPlan(tp, predNameEqAlice)

	selScan, err := sp.Open()
	require.NoError(t, err)
	defer selScan.Close()

	require.NoError(t, selScan.BeforeFirst())

	count := 0
	for {
		hasNext, err := selScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++

		// Check that name="Alice" and active=true
		nameVal, err := selScan.GetString("name")
		require.NoError(t, err)
		activeVal, err := selScan.GetBool("active")
		require.NoError(t, err)

		assert.Equal(t, "Alice", nameVal)
		assert.True(t, activeVal)
	}

	// Only 1 record matches (id=1, name="Alice", active=true)
	assert.Equal(t, 1, count, "Only 1 record should match both conditions")

	// Optional: Check DistinctValues logic for "name" or "active".
	dvName := sp.DistinctValues("name")
	assert.True(t, dvName >= 1, "Should have at least 1 distinct value for 'name'")
}
