package query

import (
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"github.com/JyotinderSingh/dropdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"

	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/table"
	"github.com/JyotinderSingh/dropdb/tx"
)

// This helper sets up a TableScan with some records we can filter on.
func setupTestTableScan(t *testing.T) (*table.Scan, func()) {
	transaction, layout, cleanup := createTransactionAndLayout(t)
	ts, err := table.NewTableScan(transaction, "selectscan_test_table", layout)
	require.NoError(t, err)

	// Insert some sample data
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

	for _, row := range data {
		require.NoError(t, ts.Insert())
		require.NoError(t, ts.SetInt("id", row.ID))
		require.NoError(t, ts.SetString("name", row.Name))
		require.NoError(t, ts.SetInt("val", row.Val))
	}
	// Move back to start so consumer can read
	require.NoError(t, ts.BeforeFirst())

	// Return the TableScan
	return ts, func() {
		cleanup()
		ts.Close()
	}
}

// createTransactionAndLayout sets up a temporary database environment,
// creates a transaction, defines a simple schema & layout, and returns them
// along with a cleanup function to free resources.
func createTransactionAndLayout(t *testing.T) (*tx.Transaction, *record.Layout, func()) {
	// Create a temporary directory for the test.
	dbDir := t.TempDir()

	// Create a file manager with a block size of 400 (example value).
	fm, err := file.NewManager(dbDir, 800)
	require.NoError(t, err, "failed to create file manager")

	// Create a log manager (storing the log in the file manager).
	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err, "failed to create log manager")

	// Create a buffer manager with a small buffer pool (e.g., 3 or 8 buffers).
	bm := buffer.NewManager(fm, lm, 3)

	// Create a lock table for concurrency control.
	lt := concurrency.NewLockTable()

	// Create a transaction.
	transaction := tx.NewTransaction(fm, lm, bm, lt)

	// Create a schema with some sample fields.
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddIntField("val")

	// Create a layout from the schema.
	layout := record.NewLayout(schema)

	// Define a cleanup function to:
	//   1) Commit the transaction
	//   2) Remove the temporary directory
	cleanup := func() {
		// Always close the transaction (commit or rollback).
		if err := transaction.Commit(); err != nil {
			t.Errorf("transaction commit failed: %v", err)
		}
		// Remove the temporary test directory.
		if err := os.RemoveAll(dbDir); err != nil {
			t.Errorf("failed to remove temp dir %s: %v", dbDir, err)
		}
	}

	return transaction, layout, cleanup
}

// ------------------------------------
// Tests
// ------------------------------------

// 1. Test a SelectScan with no predicate (should return all rows).
func TestSelectScan_NoPredicate(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// Create a SelectScan with a nil predicate.
	ss, err := NewSelectScan(ts, NewPredicate())
	require.NoError(t, err)
	defer ss.Close()

	// We expect all 4 inserted records
	count := 0
	require.NoError(t, ss.BeforeFirst())
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
	}
	assert.Equal(t, 4, count, "SelectScan with no predicate should return all rows")
}

// 2. Test a SelectScan with a predicate that matches some rows.
func TestSelectScan_SomeMatches(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// Build a predicate: val >= 30
	// We'll do something like: val >= 30
	lhsExpr := NewFieldExpression("val")
	rhsExpr := NewConstantExpression(30)
	term := NewTerm(lhsExpr, rhsExpr, types.GE)
	pred := NewPredicateFromTerm(term)

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	// Expect it to match "Carol" (val=30) and "Dave" (val=40) → 2 matches
	matches := 0
	require.NoError(t, ss.BeforeFirst())
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		val, err := ss.GetInt("val")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, val, 30)
		matches++
	}
	assert.Equal(t, 2, matches, "Should match exactly 2 rows (val=30 and val=40).")
}

// 3. Test a SelectScan with a predicate that matches no rows.
func TestSelectScan_NoMatches(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// Build a predicate: name == "Zach" (none has name=Zach)
	lhsExpr := NewFieldExpression("name")
	rhsExpr := NewConstantExpression("Zach")
	term := NewTerm(lhsExpr, rhsExpr, types.EQ)
	pred := NewPredicateFromTerm(term)

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	// Expect 0 matches
	matches := 0
	require.NoError(t, ss.BeforeFirst())
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		matches++
	}
	assert.Equal(t, 0, matches, "Predicate should match no rows.")
}

// 4. (Optional) Test updating via SelectScan if underlying scan is updatable.
func TestSelectScan_Update(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// We'll filter on, say, all rows with val < 30 (should be Alice(10), Bob(20)).
	lhsExpr := NewFieldExpression("val")
	rhsExpr := NewConstantExpression(30)
	pred := NewPredicateFromTerm(NewTerm(lhsExpr, rhsExpr, types.LT))

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	// Attempt to increment the "val" field by 100 for these matching rows.
	require.NoError(t, ss.BeforeFirst())

	updatedCount := 0
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		oldVal, err := ss.GetInt("val")
		require.NoError(t, err)

		err = ss.SetInt("val", oldVal+100)
		require.NoError(t, err)

		updatedCount++
	}

	// We expect 2 updates
	assert.Equal(t, 2, updatedCount)

	// Now verify the changes in the underlying TableScan (ts)
	// We'll scan everything and confirm the new "val" is old+100 for the matched rows.
	require.NoError(t, ts.BeforeFirst())
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := ts.GetString("name")
		require.NoError(t, err)
		val, err := ts.GetInt("val")
		require.NoError(t, err)

		switch name {
		case "Alice":
			assert.Equal(t, 110, val, "Alice was updated from 10 to 110")
		case "Bob":
			assert.Equal(t, 120, val, "Bob was updated from 20 to 120")
		case "Carol":
			assert.Equal(t, 30, val, "Carol should be unchanged")
		case "Dave":
			assert.Equal(t, 40, val, "Dave should be unchanged")
		}
	}
}

// 5. (Optional) Test that Delete works on matching rows
func TestSelectScan_Delete(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// Build a predicate that selects rows with id < 3 (Alice=1, Bob=2)
	lhsExpr := NewFieldExpression("id")
	rhsExpr := NewConstantExpression(3)
	pred := NewPredicateFromTerm(NewTerm(lhsExpr, rhsExpr, types.LT))

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	require.NoError(t, ss.BeforeFirst())

	deletedCount := 0
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		// Delete current record
		err = ss.Delete()
		require.NoError(t, err)
		deletedCount++
	}
	assert.Equal(t, 2, deletedCount, "Should have deleted 2 rows: id=1 and id=2")

	// Verify the underlying TableScan now has only Carol and Dave
	require.NoError(t, ts.BeforeFirst())

	var remainingIDs []int
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := ts.GetInt("id")
		require.NoError(t, err)
		remainingIDs = append(remainingIDs, id)
	}
	assert.ElementsMatch(t, []int{3, 4}, remainingIDs)
}

// 6. (Optional) Test GetRecordID / MoveToRecordID if relevant
func TestSelectScan_RecordID(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// We can capture the record ID of Carol (id=3).
	// Then we move with a predicate that maybe filters out Carol,
	// but we check if we can still MoveToRecordID.

	// 1) Move to Carol
	require.NoError(t, ts.BeforeFirst())
	foundCarol := false
	var carolRID *record.ID

	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := ts.GetInt("id")
		require.NoError(t, err)
		if id == 3 {
			// Found Carol
			carolRID = ts.GetRecordID()
			require.NotNil(t, carolRID)
			foundCarol = true
			break
		}
	}
	require.True(t, foundCarol, "Carol should be in the table")

	// 2) Now create a select scan with a different predicate, e.g. id < 3
	// Carol is not included in that set. Then try MoveToRecordID on Carol.
	lhsExpr := NewFieldExpression("id")
	rhsExpr := NewConstantExpression(3)
	pred := NewPredicateFromTerm(NewTerm(lhsExpr, rhsExpr, types.LT))

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	// Attempt to MoveToRecordID( carolRID ) → underlying scan supports it.
	err = ss.MoveToRecordID(carolRID)
	require.NoError(t, err)

	// We *are* at Carol's record in the underlying scan,
	// but the next call to Next() might skip it because it doesn't match the predicate.
	// That depends on your usage. For demonstration, let's see what's currently in the record:
	idVal, err := ss.GetInt("id")
	require.NoError(t, err)
	// Because we've forced the underlying TableScan to Carol's record,
	// we can read it *if* the code doesn't check the predicate *immediately* on MoveToRecordID.

	assert.Equal(t, 3, idVal, "We forcibly moved to Carol, even though the predicate wouldn't match in normal iteration.")
}

// TestSelectScan_OperatorNE tests the "!=" (not equals) operator.
// We expect rows where `val != 20` → i.e., val = 10, 30, 40 → Alice, Carol, Dave.
func TestSelectScan_OperatorNE(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	lhsExpr := NewFieldExpression("val")
	rhsExpr := NewConstantExpression(20)
	term := NewTerm(lhsExpr, rhsExpr, types.NE)
	pred := NewPredicateFromTerm(term)

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	require.NoError(t, ss.BeforeFirst())

	var matchedNames []string
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := ss.GetString("name")
		require.NoError(t, err)
		matchedNames = append(matchedNames, name)
	}

	// We expect ["Alice", "Carol", "Dave"] (3 records).
	assert.ElementsMatch(t, []string{"Alice", "Carol", "Dave"}, matchedNames)
}

// TestSelectScan_OperatorGT tests the ">" (greater than) operator.
// We expect rows where `id > 2` → i.e., id=3 ("Carol"), and id=4 ("Dave").
func TestSelectScan_OperatorGT(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	lhsExpr := NewFieldExpression("id")
	rhsExpr := NewConstantExpression(2)
	term := NewTerm(lhsExpr, rhsExpr, types.GT)
	pred := NewPredicateFromTerm(term)

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	require.NoError(t, ss.BeforeFirst())

	var matchedIDs []int
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		id, err := ss.GetInt("id")
		require.NoError(t, err)
		matchedIDs = append(matchedIDs, id)
	}

	assert.ElementsMatch(t, []int{3, 4}, matchedIDs)
}

// TestSelectScan_OperatorLE tests the "<=" (less than or equal) operator.
// We'll do `val <= 20`, which should match Alice (val=10) and Bob (val=20).
func TestSelectScan_OperatorLE(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	lhsExpr := NewFieldExpression("val")
	rhsExpr := NewConstantExpression(20)
	term := NewTerm(lhsExpr, rhsExpr, types.LE)
	pred := NewPredicateFromTerm(term)

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	require.NoError(t, ss.BeforeFirst())

	var matched []string
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := ss.GetString("name")
		require.NoError(t, err)
		matched = append(matched, name)
	}

	assert.ElementsMatch(t, []string{"Alice", "Bob"}, matched)
}

// TestSelectScan_MultipleTerms demonstrates a composite predicate (logical AND),
// e.g., `val > 10 AND val < 40` → should match `val = 20` (Bob) and `val = 30` (Carol).
func TestSelectScan_MultipleTerms(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// First term: val > 10
	term1 := NewTerm(NewFieldExpression("val"), NewConstantExpression(10), types.GT)
	// Second term: val < 40
	term2 := NewTerm(NewFieldExpression("val"), NewConstantExpression(40), types.LT)

	// We combine them into a single predicate: (val > 10) AND (val < 40).
	pred := NewPredicateFromTerm(term1)
	pred.ConjoinWith(NewPredicateFromTerm(term2))

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	require.NoError(t, ss.BeforeFirst())

	var matchedNames []string
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := ss.GetString("name")
		require.NoError(t, err)
		matchedNames = append(matchedNames, name)
	}

	// Expect Bob (val=20) and Carol (val=30).
	assert.ElementsMatch(t, []string{"Bob", "Carol"}, matchedNames)
}

// TestSelectScan_FieldVsField compares two fields, e.g. `val > id`.
// Using the data: (1, "Alice", 10), (2, "Bob", 20), (3, "Carol", 30), (4, "Dave", 40)
//   - For Alice: val(10) > id(1) => 10 > 1 → true
//   - Bob:  20 > 2  => true
//   - Carol: 30 > 3 => true
//   - Dave:  40 > 4 => true
//
// Actually, in this dataset, val is always 10× the id, so all should match.
// Let's illustrate the usage anyway.
func TestSelectScan_FieldVsField(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// insert a new record with id > val
	require.NoError(t, ts.Insert())
	require.NoError(t, ts.SetInt("id", 500))
	require.NoError(t, ts.SetString("name", "Eve"))
	require.NoError(t, ts.SetInt("val", 50))

	// Left = "val" field, Right = "id" field
	lhsExpr := NewFieldExpression("val")
	rhsExpr := NewFieldExpression("id")
	term := NewTerm(lhsExpr, rhsExpr, types.GT) // val > id
	pred := NewPredicateFromTerm(term)

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	require.NoError(t, ss.BeforeFirst())

	// Expect all 4 to match (since 10>1, 20>2, 30>3, 40>4).
	var matches []string
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := ss.GetString("name")
		require.NoError(t, err)
		matches = append(matches, name)
	}
	assert.ElementsMatch(t, []string{"Alice", "Bob", "Carol", "Dave"}, matches)

	// Validate reverse predicate
	lhsExpr = NewFieldExpression("id")
	rhsExpr = NewFieldExpression("val")
	term = NewTerm(lhsExpr, rhsExpr, types.GT) // id > val
	pred = NewPredicateFromTerm(term)

	ss, err = NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	require.NoError(t, ss.BeforeFirst())

	// Expect only Eve to match
	matches = nil
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := ss.GetString("name")
		require.NoError(t, err)
		matches = append(matches, name)
	}
	assert.ElementsMatch(t, []string{"Eve"}, matches)
}

// TestSelectScan_StringComparison can test string comparison with <, >, etc.
// For example: name < "Carol". That should match Alice and Bob (lexically).
func TestSelectScan_StringComparison(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// We'll do name < "Carol" → "Alice" < "Carol", "Bob" < "Carol", but "Carol" == "Carol", "Dave" > "Carol".
	lhsExpr := NewFieldExpression("name")
	rhsExpr := NewConstantExpression("Carol")
	term := NewTerm(lhsExpr, rhsExpr, types.LT)
	pred := NewPredicateFromTerm(term)

	ss, err := NewSelectScan(ts, pred)
	require.NoError(t, err)
	defer ss.Close()

	require.NoError(t, ss.BeforeFirst())

	var matchedNames []string
	for {
		hasNext, err := ss.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := ss.GetString("name")
		require.NoError(t, err)
		matchedNames = append(matchedNames, name)
	}

	// Expect "Alice" and "Bob".
	assert.ElementsMatch(t, []string{"Alice", "Bob"}, matchedNames)
}
