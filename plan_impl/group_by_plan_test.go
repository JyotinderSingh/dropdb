package plan_impl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/JyotinderSingh/dropdb/query/functions"
	"github.com/JyotinderSingh/dropdb/scan"
)

// ----------------------------------------------------------------------
// Test #1: No grouping fields => entire table is treated as one group
// ----------------------------------------------------------------------
// We'll demonstrate a single aggregator: MaxFunction on "salary".
// The entire table is effectively one group, so we expect exactly one output record.
func TestGroupByPlan_NoGroupFields(t *testing.T) {
	// 1) Setup the environment
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	// 2) Create table schema: "dept" (string), "salary" (int)
	mdm := createTableMetadataWithSchema(t, txn, "employees", map[string]interface{}{
		"dept":   "string",
		"salary": 0,
	})

	// 3) Insert sample data
	tp, err := NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	sampleData := []map[string]interface{}{
		{"dept": "Sales", "salary": 1000},
		{"dept": "Sales", "salary": 2000},
		{"dept": "Marketing", "salary": 1500},
		{"dept": "Engineering", "salary": 3000},
		{"dept": "Marketing", "salary": 1800},
	}
	insertRecords(t, us, sampleData)

	// Reopen table plan to refresh stats
	tp, err = NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	// 4) Create a GroupByPlan with NO group fields, aggregator: maxOfsalary
	maxFn := functions.NewMaxFunction("salary")
	gbPlan := NewGroupByPlan(txn, tp, []string{}, []functions.AggregationFunction{maxFn})

	// 5) Check the schema
	//    - We expect one field: maxOfsalary
	schema := gbPlan.Schema()
	require.NotNil(t, schema)
	assert.True(t, schema.HasField("maxOfsalary"))

	// 6) Open the group-by plan
	gbScan, err := gbPlan.Open()
	require.NoError(t, err)
	defer gbScan.Close()

	// 7) We expect exactly ONE group result
	require.NoError(t, gbScan.BeforeFirst())
	hasNext, err := gbScan.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	// Because it's the entire table, maxOfsalary should be 3000
	maxVal, err := gbScan.GetInt(maxFn.FieldName()) // e.g. "maxOfsalary"
	require.NoError(t, err)
	assert.Equal(t, 3000, maxVal)

	// No second group
	hasNext, err = gbScan.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)

	// 8) Check plan-level stats (optional)
	//    - BlocksAccessed is typically the underlying plan's cost
	//    - RecordsOutput is typically 1 in no-group scenario
	assert.Equal(t, tp.BlocksAccessed(), gbPlan.BlocksAccessed())
	assert.Equal(t, 1, gbPlan.RecordsOutput())
}

// ----------------------------------------------------------------------
// Test #2: Single grouping field => "dept".
// We'll do a single aggregator: MaxFunction("salary").
// We should see one result row per department.
// ----------------------------------------------------------------------
func TestGroupByPlan_SingleGroupField(t *testing.T) {
	// 1) Setup environment
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	// 2) Create table
	mdm := createTableMetadataWithSchema(t, txn, "employees2", map[string]interface{}{
		"dept":   "string",
		"salary": 0,
	})

	// 3) Insert sample data
	tp, err := NewTablePlan(txn, "employees2", mdm)
	require.NoError(t, err)
	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	sampleData := []map[string]interface{}{
		{"dept": "Engineering", "salary": 3000},
		{"dept": "Sales", "salary": 1000},
		{"dept": "Marketing", "salary": 1500},
		{"dept": "Marketing", "salary": 1800},
		{"dept": "Engineering", "salary": 2500},
		{"dept": "Sales", "salary": 2000},
	}
	insertRecords(t, us, sampleData)

	// Reopen table plan
	tp, err = NewTablePlan(txn, "employees2", mdm)
	require.NoError(t, err)

	// 4) GroupByPlan: group by "dept", aggregator = maxOfsalary
	maxFn := functions.NewMaxFunction("salary")
	gbPlan := NewGroupByPlan(txn, tp, []string{"dept"}, []functions.AggregationFunction{maxFn})

	// 5) Check Schema
	//    - We expect two fields: "dept" and "maxOfsalary"
	schema := gbPlan.Schema()
	require.NotNil(t, schema, "Schema should not be nil")
	assert.True(t, schema.HasField("dept"))
	assert.True(t, schema.HasField("maxOfsalary"))

	// 6) Open and iterate results
	gbScan, err := gbPlan.Open()
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())

	// We'll store results in a map: dept -> maxSalary
	deptToMax := make(map[string]int)
	for {
		hasNext, err := gbScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		deptVal, err := gbScan.GetString("dept")
		require.NoError(t, err)

		maxVal, err := gbScan.GetInt(maxFn.FieldName()) // "maxOfsalary"
		require.NoError(t, err)

		deptToMax[deptVal] = maxVal
	}

	// We expect 3 departments
	assert.Equal(t, 3, len(deptToMax))
	// Engineering -> max=3000
	assert.Equal(t, 3000, deptToMax["Engineering"])
	// Marketing -> max=1800
	assert.Equal(t, 1800, deptToMax["Marketing"])
	// Sales -> max=2000
	assert.Equal(t, 2000, deptToMax["Sales"])

	// Plan-level checks
	//   - We expect 3 groups => RecordsOutput should be around 3
	assert.Equal(t, 3, gbPlan.RecordsOutput())
}

// ----------------------------------------------------------------------
// Test #3: Single grouping field = "dept" again, but *multiple* aggregations
// We'll show MaxFunction("salary") plus CountFunction("salary") for demonstration.
// ----------------------------------------------------------------------
func TestGroupByPlan_MultipleAggregators(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	// Create table
	mdm := createTableMetadataWithSchema(t, txn, "employees3", map[string]interface{}{
		"dept":   "string",
		"salary": 0,
	})

	// Insert data
	tp, err := NewTablePlan(txn, "employees3", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	// Notice that we have multiple rows in same dept
	data := []map[string]interface{}{
		{"dept": "Engineering", "salary": 3000},
		{"dept": "Engineering", "salary": 2500},
		{"dept": "Sales", "salary": 1000},
		{"dept": "Sales", "salary": 2000},
		{"dept": "Marketing", "salary": 1500},
		{"dept": "Marketing", "salary": 1800},
		{"dept": "Sales", "salary": 1800},
	}
	insertRecords(t, us, data)

	// Reopen
	tp, err = NewTablePlan(txn, "employees3", mdm)
	require.NoError(t, err)

	// 4) Create a GroupByPlan with groupFields=["dept"], aggregators: MaxFunction + CountFunction
	maxFn := functions.NewMaxFunction("salary")
	countFn := functions.NewCountFunction("salary") // or "countOf(salary)"
	gbPlan := NewGroupByPlan(txn, tp, []string{"dept"}, []functions.AggregationFunction{maxFn, countFn})

	// 5) Check Schema
	//    - We expect three fields: "dept", "maxOfsalary", "countOfsalary"
	schema := gbPlan.Schema()
	require.NotNil(t, schema)
	assert.True(t, schema.HasField("dept"))
	assert.True(t, schema.HasField("maxOfsalary"))
	assert.True(t, schema.HasField("countOfsalary"))

	// 5) Open and iterate
	gbScan, err := gbPlan.Open()
	require.NoError(t, err)
	defer gbScan.Close()

	require.NoError(t, gbScan.BeforeFirst())

	// We'll store dept -> (maxSalary, count)
	type result struct {
		maxSalary int
		count     int64
	}
	results := make(map[string]result)

	for {
		hasNext, err := gbScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		deptVal, err := gbScan.GetString("dept")
		require.NoError(t, err)

		maxVal, err := gbScan.GetInt(maxFn.FieldName()) // "maxOfsalary"
		require.NoError(t, err)

		countValAny, err := gbScan.GetVal(countFn.FieldName()) // "countOfsalary"
		require.NoError(t, err)

		countVal, ok := countValAny.(int64)
		require.True(t, ok)

		results[deptVal] = result{maxVal, countVal}
	}

	// 6) Assert
	// Expect 3 distinct dept groups
	require.Equal(t, 3, len(results))

	// Engineering => salaries [3000, 2500], max=3000, count=2
	eng := results["Engineering"]
	assert.Equal(t, 3000, eng.maxSalary)
	assert.EqualValues(t, 2, eng.count)

	// Sales => salaries [1000, 2000, 1800], max=2000, count=3
	sales := results["Sales"]
	assert.Equal(t, 2000, sales.maxSalary)
	assert.EqualValues(t, 3, sales.count)

	// Marketing => salaries [1500, 1800], max=1800, count=2
	mkt := results["Marketing"]
	assert.Equal(t, 1800, mkt.maxSalary)
	assert.EqualValues(t, 2, mkt.count)
}
