package query_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/query/functions"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/table"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
)

func setupGroupByTestTableScan(t *testing.T) (*table.Scan, func()) {
	transaction, layout, cleanup := createGroupByTransactionAndLayout(t)
	ts, err := table.NewTableScan(transaction, "groupbyscan_test_table", layout)
	require.NoError(t, err)

	// Insert some sample data with duplicates for grouping
	data := []struct {
		Dept   string
		Salary int
	}{
		// Note: Underlying data needs to be sorted by "dept" for grouping to work.
		// We'll rely on the order of insertion to keep it sorted.
		{"Sales", 1000},
		{"Sales", 2000},
		{"Sales", 1800},
		{"Marketing", 1500},
		{"Marketing", 1500},
		{"Engineering", 2500},
		{"Engineering", 3000},
	}

	for _, row := range data {
		require.NoError(t, ts.Insert())
		require.NoError(t, ts.SetString("dept", row.Dept))
		require.NoError(t, ts.SetInt("salary", row.Salary))
	}
	// Move back to start so consumer can read from the beginning
	require.NoError(t, ts.BeforeFirst())

	// Return the TableScan
	return ts, func() {
		cleanup()
		ts.Close()
	}
}

func createGroupByTransactionAndLayout(t *testing.T) (*tx.Transaction, *record.Layout, func()) {
	dbDir := t.TempDir()

	fm, err := file.NewManager(dbDir, 400)
	require.NoError(t, err, "failed to create file manager")

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err, "failed to create log manager")

	bm := buffer.NewManager(fm, lm, 3)
	lt := concurrency.NewLockTable()
	transaction := tx.NewTransaction(fm, lm, bm, lt)

	// Create a schema with grouping-friendly fields.
	schema := record.NewSchema()
	schema.AddStringField("dept", 20)
	schema.AddIntField("salary")

	layout := record.NewLayout(schema)

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

//  1. Test grouping with NO group fields (i.e., entire table is one group).
//     We'll use a MaxFunction on "salary" to illustrate a single aggregator.
func TestGroupByScan_NoGroupFields(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	// aggregator: maxOfsalary
	maxSalaryFn := functions.NewMaxFunction("salary")

	// Create a GroupByScan with no group fields => everything in one group
	gbScan, err := query.NewGroupByScan(
		ts,
		[]string{}, // no group fields
		[]functions.AggregationFunction{maxSalaryFn},
	)
	require.NoError(t, err)
	defer gbScan.Close()

	// We expect exactly 1 group result with a single aggregator field: "maxOfsalary"
	require.NoError(t, gbScan.BeforeFirst())
	hasNext, err := gbScan.Next()
	require.NoError(t, err)
	require.True(t, hasNext, "Should be exactly one group")

	// Now let's see the aggregator's result
	maxVal, err := gbScan.GetInt(maxSalaryFn.FieldName()) // "maxOfsalary"
	require.NoError(t, err)
	// Given the data: [1000, 2000, 1500, 1500, 2500, 3000, 1800], max = 3000
	assert.Equal(t, 3000, maxVal, "The max salary in the entire table should be 3000")

	// No more groups
	hasNext, err = gbScan.Next()
	require.NoError(t, err)
	assert.False(t, hasNext, "Only one group expected with no grouping fields.")
}

//  2. Test grouping on a single field: "dept". Then use Max(salary).
//     We expect each distinct "dept" as a group and a "maxOfsalary".
func TestGroupByScan_SingleGroupField(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	maxSalaryFn := functions.NewMaxFunction("salary")

	gbScan, err := query.NewGroupByScan(
		ts,
		[]string{"dept"}, // group by dept
		[]functions.AggregationFunction{maxSalaryFn},
	)
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())

	// We'll collect the group field "dept" and aggregator "maxOfsalary" for each group.
	deptToMax := make(map[string]int)
	for {
		hasNext, err := gbScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		// group field
		deptVal, err := gbScan.GetString("dept")
		require.NoError(t, err)

		// aggregator field
		maxVal, err := gbScan.GetInt(maxSalaryFn.FieldName()) // "maxOfsalary"
		require.NoError(t, err)

		deptToMax[deptVal] = maxVal
	}

	// We inserted:
	//   Sales: 1000, 2000, 1800 -> max = 2000
	//   Marketing: 1500, 1500 -> max = 1500
	//   Engineering: 2500, 3000 -> max = 3000
	assert.Equal(t, 3, len(deptToMax), "We expect 3 distinct dept groups")
	assert.Equal(t, 2000, deptToMax["Sales"])
	assert.Equal(t, 1500, deptToMax["Marketing"])
	assert.Equal(t, 3000, deptToMax["Engineering"])
}

// 3) Test HasField() logic: group fields + aggregator fields should be recognized.
func TestGroupByScan_HasField(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	maxSalaryFn := functions.NewMaxFunction("salary")
	gbScan, err := query.NewGroupByScan(
		ts,
		[]string{"dept"},
		[]functions.AggregationFunction{maxSalaryFn},
	)
	require.NoError(t, err)
	defer gbScan.Close()

	assert.True(t, gbScan.HasField("dept"), "GroupByScan should have the group field 'dept'")
	assert.True(t, gbScan.HasField("maxOfsalary"), "GroupByScan should have aggregator field 'maxOfsalary'")

	// Original underlying fields that are not grouped or aggregated
	// won't necessarily appear as "HasField" in GroupByScan,
	// unless they are used as group fields or aggregator fields.
	// So 'salary' by itself is not directly accessible (unless you implement differently).
	assert.False(t, gbScan.HasField("salary"), "Should not have direct access to 'salary' unless it's a group field or aggregator output.")
}

//  4. Test error when accessing a non-existent or invalid field
//     (like if you do gbScan.GetInt("noSuchField")).
func TestGroupByScan_FieldNotFound(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	maxSalaryFn := functions.NewMaxFunction("salary")
	gbScan, err := query.NewGroupByScan(
		ts,
		[]string{"dept"},
		[]functions.AggregationFunction{maxSalaryFn},
	)
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())
	hasNext, err := gbScan.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	_, err = gbScan.GetInt("someRandomField")
	assert.Error(t, err, "Accessing a field not in group fields or aggregator fields should return an error")
}

//  5. Test multiple aggregators (if you have them). For demo, we reuse Max twice.
//     In a real scenario, you'd have e.g. Max & Min, or Max & Count, etc.
func TestGroupByScan_MultipleAggregators(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	// For demonstration, we'll create two MaxFunctions:
	//  (1) maxOfsalary (int)
	//  (2) maxOfdept (string, lexicographically)
	maxSalaryFn := functions.NewMaxFunction("salary")
	maxDeptFn := functions.NewMaxFunction("dept")

	gbScan, err := query.NewGroupByScan(
		ts,
		[]string{"dept"}, // group by "dept"
		[]functions.AggregationFunction{maxSalaryFn, maxDeptFn},
	)
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())

	// We'll collect the resulting groups in a map:
	//   groupDept -> {maxSalary, maxDept}
	type aggResult struct {
		maxSalary int
		maxDept   string
	}
	results := make(map[string]aggResult)

	for {
		hasNext, err := gbScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		groupDept, err := gbScan.GetString("dept")
		require.NoError(t, err)

		maxSalary, err := gbScan.GetInt(maxSalaryFn.FieldName()) // "maxOfsalary"
		require.NoError(t, err)

		maxDept, err := gbScan.GetString(maxDeptFn.FieldName()) // "maxOfdept"
		require.NoError(t, err)

		// Store in a map so we can assert after collecting everything
		results[groupDept] = aggResult{maxSalary, maxDept}
	}

	// 1) We expect exactly 3 groups in the data
	assert.Equal(t, 3, len(results), "Expected exactly 3 dept groups")

	// 2) Check each group's aggregator values
	//    For example, if you inserted (in sorted order):
	//    Engineering: salaries = [2500, 3000], Marketing: [1500,1500], Sales: [1000,1800,2000]
	//    => maxSalary = 3000 (Engineering), 1500 (Marketing), 2000 (Sales)
	//    => maxDept  should remain the lexicographically largest string among them (which is
	//       just the same name repeated, but we'll still assert).

	// Engineering
	eng, ok := results["Engineering"]
	require.True(t, ok, "Engineering group not found in results")
	assert.Equal(t, 3000, eng.maxSalary, "maxOfsalary for Engineering should be 3000")
	assert.Equal(t, "Engineering", eng.maxDept, "maxOfdept for Engineering should be 'Engineering'")

	// Marketing
	mkt, ok := results["Marketing"]
	require.True(t, ok, "Marketing group not found in results")
	assert.Equal(t, 1500, mkt.maxSalary, "maxOfsalary for Marketing should be 1500")
	assert.Equal(t, "Marketing", mkt.maxDept, "maxOfdept for Marketing should be 'Marketing'")

	// Sales
	sales, ok := results["Sales"]
	require.True(t, ok, "Sales group not found in results")
	assert.Equal(t, 2000, sales.maxSalary, "maxOfsalary for Sales should be 2000")
	assert.Equal(t, "Sales", sales.maxDept, "maxOfdept for Sales should be 'Sales'")
}

//  6. Test reading the aggregator values across all groups to confirm grouping properly
//     lumps records with the same "dept" together. Essentially same as #2, but we re-check
//     that we can iterate *all* groups fully.
func TestGroupByScan_MultipleGroupsIteration(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	maxSalaryFn := functions.NewMaxFunction("salary")
	gbScan, err := query.NewGroupByScan(
		ts,
		[]string{"dept"},
		[]functions.AggregationFunction{maxSalaryFn},
	)
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())

	groupsCount := 0
	for {
		hasNext, err := gbScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		groupsCount++
		// We won't re-verify everything here, just confirm we can iterate through.
	}

	// We inserted 3 distinct "dept": Sales, Marketing, Engineering
	assert.Equal(t, 3, groupsCount, "Should have 3 distinct groups.")
}

func TestGroupByScan_MinFunction(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	// Create a MinFunction aggregator for "salary"
	minFn := functions.NewMinFunction("salary")

	// Group by "dept", using Min(salary)
	gbScan, err := query.NewGroupByScan(ts, []string{"dept"}, []functions.AggregationFunction{minFn})
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())

	// We'll store dept -> minOfsalary
	results := make(map[string]int)

	for {
		hasNext, err := gbScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		dept, err := gbScan.GetString("dept")
		require.NoError(t, err)

		minVal, err := gbScan.GetInt(minFn.FieldName()) // e.g. "minOfsalary"
		require.NoError(t, err)

		results[dept] = minVal
	}

	// Verify we got the correct number of groups.
	// If your table has exactly 3 depts: Engineering, Marketing, Sales:
	assert.Equal(t, 3, len(results), "Should have 3 distinct dept groups")

	// Based on the sample data (Engineering: [2500,3000], Marketing: [1500,1500], Sales: [1000,1800,2000])
	// => minOfsalary = 2500 (Eng), 1500 (Marketing), 1000 (Sales)
	assert.Equal(t, 2500, results["Engineering"], "Engineering min salary should be 2500")
	assert.Equal(t, 1500, results["Marketing"], "Marketing min salary should be 1500")
	assert.Equal(t, 1000, results["Sales"], "Sales min salary should be 1000")
}

func TestGroupByScan_SumFunction(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	// Create a SumFunction aggregator for "salary"
	sumFn := functions.NewSumFunction("salary")

	// Group by "dept", using Sum(salary)
	gbScan, err := query.NewGroupByScan(ts, []string{"dept"}, []functions.AggregationFunction{sumFn})
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())

	// We'll store dept -> sumOfsalary
	results := make(map[string]int64)

	for {
		hasNext, err := gbScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		dept, err := gbScan.GetString("dept")
		require.NoError(t, err)

		sumValAny, err := gbScan.GetVal(sumFn.FieldName()) // "sumOfsalary"
		require.NoError(t, err)

		// sumOfsalary is stored as int64 in SumFunction
		sumVal, ok := sumValAny.(int64)
		require.True(t, ok, "sumOfsalary should be an int64")

		results[dept] = sumVal
	}

	assert.Equal(t, 3, len(results), "Should have 3 distinct dept groups")

	assert.EqualValues(t, 5500, results["Engineering"], "Engineering sum should be 5500")
	assert.EqualValues(t, 3000, results["Marketing"], "Marketing sum should be 3000")
	assert.EqualValues(t, 4800, results["Sales"], "Sales sum should be 4800")
}

func TestGroupByScan_CountFunction(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	// Create a CountFunction aggregator for "salary"
	// (though it often doesn't matter which field if counting rows)
	countFn := functions.NewCountFunction("salary")

	// Group by "dept", using Count(salary)
	gbScan, err := query.NewGroupByScan(ts, []string{"dept"}, []functions.AggregationFunction{countFn})
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())

	results := make(map[string]int64)

	for {
		hasNext, err := gbScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		dept, err := gbScan.GetString("dept")
		require.NoError(t, err)

		countValAny, err := gbScan.GetVal(countFn.FieldName()) // "countOfsalary"
		require.NoError(t, err)

		countVal, ok := countValAny.(int64)
		require.True(t, ok, "countOfsalary should be int64")

		results[dept] = countVal
	}

	assert.Equal(t, 3, len(results), "Should have 3 distinct dept groups")

	//  Engineering: 2 rows, Marketing: 2 rows, Sales: 3 rows
	// => countOfsalary => 2, 2, 3
	assert.EqualValues(t, 2, results["Engineering"], "Engineering should have 2 rows")
	assert.EqualValues(t, 2, results["Marketing"], "Marketing should have 2 rows")
	assert.EqualValues(t, 3, results["Sales"], "Sales should have 3 rows")
}

func TestGroupByScan_AvgFunction(t *testing.T) {
	ts, cleanup := setupGroupByTestTableScan(t)
	defer cleanup()

	// Create an AvgFunction aggregator for "salary"
	avgFn := functions.NewAvgFunction("salary")

	// Group by "dept", using Avg(salary)
	gbScan, err := query.NewGroupByScan(ts, []string{"dept"}, []functions.AggregationFunction{avgFn})
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())

	results := make(map[string]float64)

	for {
		hasNext, err := gbScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		dept, err := gbScan.GetString("dept")
		require.NoError(t, err)

		// "avgOfsalary" -> float64
		avgValAny, err := gbScan.GetVal(avgFn.FieldName())
		require.NoError(t, err)

		avgVal, ok := avgValAny.(float64)
		require.True(t, ok, "avgOfsalary should be float64")

		results[dept] = avgVal
	}

	assert.Equal(t, 3, len(results), "Should have 3 distinct dept groups")

	//   Engineering: (2500 + 3000)/2 = 2750.0
	//   Marketing: (1500 + 1500)/2 = 1500.0
	//   Sales: (1000 + 1800 + 2000)/3 = 4800/3 = 1600.0
	assert.InDelta(t, 2750.0, results["Engineering"], 0.0001, "Engineering avg should be ~2750.0")
	assert.InDelta(t, 1500.0, results["Marketing"], 0.0001, "Marketing avg should be ~1500.0")
	assert.InDelta(t, 1600.0, results["Sales"], 0.0001, "Sales avg should be ~1600.0")
}
