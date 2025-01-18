package plan_impl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/JyotinderSingh/dropdb/scan"
)

func TestProductPlan_Basic(t *testing.T) {
	// 1) Setup environment
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	// 2) Create two tables, "departments" and "employees"

	// Table "departments": fields -> (dept_id INT, dept_name STRING)
	mdm := createTableWithSchema(t, txn, "departments", map[string]interface{}{
		"dept_id":   0,
		"dept_name": "string",
	})

	// Table "employees": fields -> (emp_id INT, emp_name STRING, dept_id INT)
	createTableWithSchema(t, txn, "employees", map[string]interface{}{
		"emp_id":   0,
		"emp_name": "string",
		"dept_id":  0,
	})

	// 3) Create TablePlans to insert records in both tables
	deptPlan, err := NewTablePlan(txn, "departments", mdm)
	require.NoError(t, err)

	s1, err := deptPlan.Open()
	require.NoError(t, err)
	defer s1.Close()

	us1, ok := s1.(scan.UpdateScan)
	require.True(t, ok)

	// Insert departments
	deptRecords := []map[string]interface{}{
		{"dept_id": 10, "dept_name": "Engineering"},
		{"dept_id": 20, "dept_name": "Marketing"},
		{"dept_id": 30, "dept_name": "Finance"},
	}
	insertRecords(t, us1, deptRecords)

	// Create a TablePlan for "employees"
	empPlan, err := NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	s2, err := empPlan.Open()
	require.NoError(t, err)
	defer s2.Close()

	us2, ok := s2.(scan.UpdateScan)
	require.True(t, ok)

	// Insert employees
	empRecords := []map[string]interface{}{
		{"emp_id": 1, "emp_name": "Alice", "dept_id": 10},
		{"emp_id": 2, "emp_name": "Bob", "dept_id": 20},
		{"emp_id": 3, "emp_name": "Carol", "dept_id": 20},
		{"emp_id": 4, "emp_name": "Dan", "dept_id": 30},
	}
	insertRecords(t, us2, empRecords)

	// Re-instantiate the plans so stats are up to date
	deptPlan, err = NewTablePlan(txn, "departments", mdm)
	require.NoError(t, err)
	empPlan, err = NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	// 4) Create a ProductPlan that combines "departments" x "employees"
	productPlan, err := NewProductPlan(deptPlan, empPlan)
	require.NoError(t, err)

	// 5) Open the ProductPlan and verify the cross-join (Cartesian product)
	productScan, err := productPlan.Open()
	require.NoError(t, err)
	defer productScan.Close()

	require.NoError(t, productScan.BeforeFirst())

	// Because we have 3 departments and 4 employees, the product should yield 12 rows.
	expectedCount := len(deptRecords) * len(empRecords)
	actualCount := 0

	for {
		hasNext, err := productScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		actualCount++

		// We can read fields from both "departments" and "employees"
		_, err = productScan.GetInt("dept_id")
		require.NoError(t, err)
		_, err = productScan.GetInt("emp_id")
		require.NoError(t, err)

		// Also read "dept_name" from departments
		deptName, err := productScan.GetString("dept_name")
		require.NoError(t, err)

		// Also read "emp_name" from employees
		empName, err := productScan.GetString("emp_name")
		require.NoError(t, err)

		// We won't do a deep check on matching dept_id’s or anything,
		// because a pure product means we don't have a join condition.
		// We just confirm these are valid sets from each table.
		// But you can do extra checks if you like.
		assert.NotEmpty(t, deptName)
		assert.NotEmpty(t, empName)
		// deptID and empID should come from the respective table's inserted data
	}
	assert.Equal(t, expectedCount, actualCount, "Product should have 3 x 4 = 12 rows")

	// 6) Verify stats: see doc string for formula
	blocks1 := deptPlan.BlocksAccessed()
	blocks2 := empPlan.BlocksAccessed()
	recs1 := deptPlan.RecordsOutput()
	recs2 := empPlan.RecordsOutput()

	expectedBlocks := blocks1 + recs1*blocks2
	assert.Equal(t, expectedBlocks, productPlan.BlocksAccessed(),
		"BlocksAccessed should follow formula: blocks1 + recs1*blocks2",
	)

	expectedRecsOut := recs1 * recs2
	assert.Equal(t, expectedRecsOut, productPlan.RecordsOutput(),
		"RecordsOutput should be recs1 * recs2",
	)

	// Distinct values for "dept_id" is the same as deptPlan's distinct count
	dvDeptID := productPlan.DistinctValues("dept_id")
	assert.Equal(t, deptPlan.DistinctValues("dept_id"), dvDeptID)

	// Distinct values for "emp_name" is from empPlan
	dvEmpName := productPlan.DistinctValues("emp_name")
	assert.Equal(t, empPlan.DistinctValues("emp_name"), dvEmpName)

	// 7) Verify schema is union of both "departments" and "employees" fields
	productSchema := productPlan.Schema()
	require.NotNil(t, productSchema)

	assert.True(t, productSchema.HasField("dept_id"))
	assert.True(t, productSchema.HasField("dept_name"))
	assert.True(t, productSchema.HasField("emp_id"))
	assert.True(t, productSchema.HasField("emp_name"))
	// Because both had a "dept_id" field, the schema just includes it once.
}
