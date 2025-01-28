package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/parse"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/tx"
)

func setupTestManagers(t *testing.T, blockSize, numBuffers int) (*file.Manager, *log.Manager, *buffer.Manager, *concurrency.LockTable) {
	dbDir := t.TempDir()
	fm, err := file.NewManager(dbDir, blockSize)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, numBuffers)
	lt := concurrency.NewLockTable()

	return fm, lm, bm, lt
}

func TestBasicQueryPlanner_SimpleSelect(t *testing.T) {
	// 1) Setup test environment
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	txn := tx.NewTransaction(fm, lm, bm, lt)

	// 2) Create the 'users' table via the metadata manager
	mdm := createTableMetadataWithSchema(t, txn, "users", map[string]interface{}{
		"id":   0,        // integer
		"name": "string", // string
		"age":  0,        // integer
	})

	// Insert a few rows
	insertTestData(t, txn, "users", mdm, []map[string]interface{}{
		{"id": 1, "name": "Alice", "age": 21},
		{"id": 2, "name": "Bob", "age": 30},
		{"id": 3, "name": "Carol", "age": 25},
	})

	require.NoError(t, txn.Commit())

	// 3) Build the BasicQueryPlanner
	qp := NewBasicQueryPlanner(mdm)

	// 4) Write a query string to parse:
	// "SELECT name FROM users WHERE id = 1"
	sql := "select name from users where id = 1"

	parser := parse.NewParser(sql)
	queryData, err := parser.Query()
	require.NoError(t, err)

	// 5) Create the plan
	// Create a transaction for the query
	queryTx := tx.NewTransaction(fm, lm, bm, lt)
	plan, err := qp.CreatePlan(queryData, queryTx)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// 6) Open the plan scan and check results
	s, err := plan.Open()
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.BeforeFirst())

	count := 0
	for {
		hasNext, err := s.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++

		// We only projected "name," so let's get that:
		nameVal, err := s.GetString("name")
		require.NoError(t, err)

		// Because we selected rows where id=1, "Alice" should appear
		assert.Equal(t, "Alice", nameVal)
	}
	// Exactly 1 row should match
	assert.Equal(t, 1, count)

	// 7) Optional: check plan-level statistics
	//    (BlocksAccessed, RecordsOutput, DistinctValues, etc.)
	//    For demonstration, just ensure everything is > 0 or reasonable:
	assert.GreaterOrEqual(t, plan.BlocksAccessed(), 1)
	assert.GreaterOrEqual(t, plan.RecordsOutput(), 1)

	require.NoError(t, queryTx.Commit())
}

// Example test that uses two tables and a join condition in the WHERE clause.
// (Note that BasicQueryPlanner does a product plus selection, so this effectively
// acts like a "join" if you specify a condition that matches across tables.)
func TestBasicQueryPlanner_JoinLikeCondition(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	txn := tx.NewTransaction(fm, lm, bm, lt)

	mdm := createTableMetadataWithSchema(t, txn, "users", map[string]interface{}{
		"id":            0,
		"name":          "string",
		"users_dept_id": 0,
	})
	mdm2 := createTableMetadataWithSchema(t, txn, "departments", map[string]interface{}{
		"dept_id":   0,
		"dept_name": "string",
	})

	// Insert some rows in "users"
	insertTestData(t, txn, "users", mdm, []map[string]interface{}{
		{"id": 1, "name": "Alice", "users_dept_id": 10},
		{"id": 2, "name": "Bob", "users_dept_id": 20},
	})

	// Insert some rows in "departments"
	insertTestData(t, txn, "departments", mdm2, []map[string]interface{}{
		{"dept_id": 10, "dept_name": "Engineering"},
		{"dept_id": 30, "dept_name": "Sales"},
	})

	require.NoError(t, txn.Commit())

	qp := NewBasicQueryPlanner(mdm)

	// A multi-table query with a WHERE condition that resembles a join:
	// SELECT name, dept_name
	// FROM users, departments
	// WHERE users.dept_id = departments.dept_id
	sql := `
        select name, dept_name
        from users, departments
        where users_dept_id = dept_id
    `
	parser := parse.NewParser(sql)
	queryData, err := parser.Query()
	require.NoError(t, err)

	queryTx := tx.NewTransaction(fm, lm, bm, lt)

	plan, err := qp.CreatePlan(queryData, queryTx)
	require.NoError(t, err)

	s, err := plan.Open()
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.BeforeFirst())

	// Because only one "dept_id" matches between the two tables (dept_id=10),
	// we expect 1 joined row: (Alice, Engineering).
	count := 0
	for {
		hasNext, err := s.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++

		nameVal, err := s.GetString("name")
		require.NoError(t, err)

		deptNameVal, err := s.GetString("dept_name")
		require.NoError(t, err)

		assert.Equal(t, "Alice", nameVal)
		assert.Equal(t, "Engineering", deptNameVal)
	}
	assert.Equal(t, 1, count)

	// Stats checks are optional
	assert.GreaterOrEqual(t, plan.BlocksAccessed(), 1)
	assert.GreaterOrEqual(t, plan.RecordsOutput(), 1)

	require.NoError(t, queryTx.Commit())
}

// Helper to insert data using a TablePlan.
func insertTestData(t *testing.T, txn *tx.Transaction, tableName string, mdm *metadata.Manager, rows []map[string]interface{}) {
	tp, err := NewTablePlan(txn, tableName, mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	for _, row := range rows {
		require.NoError(t, us.Insert())
		for fieldName, val := range row {
			switch x := val.(type) {
			case int:
				require.NoError(t, us.SetInt(fieldName, x))
			case string:
				require.NoError(t, us.SetString(fieldName, x))
			case bool:
				require.NoError(t, us.SetBool(fieldName, x))
			default:
				t.Fatalf("Unsupported value type for %s: %T", fieldName, val)
			}
		}
	}
}
func TestBasicQueryPlanner_GroupBy(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	txn := tx.NewTransaction(fm, lm, bm, lt)

	mdm := createTableMetadataWithSchema(t, txn, "employees", map[string]interface{}{
		"id":     0,
		"dept":   "string",
		"salary": 0,
	})

	insertTestData(t, txn, "employees", mdm, []map[string]interface{}{
		{"id": 1, "dept": "Engineering", "salary": 80000},
		{"id": 2, "dept": "Engineering", "salary": 90000},
		{"id": 3, "dept": "Sales", "salary": 60000},
		{"id": 4, "dept": "Sales", "salary": 65000},
	})

	require.NoError(t, txn.Commit())

	qp := NewBasicQueryPlanner(mdm)

	// Test GROUP BY with aggregate function
	sql := `
			select dept, avg(salary) 
			from employees 
			group by dept
	`
	parser := parse.NewParser(sql)
	queryData, err := parser.Query()
	require.NoError(t, err)

	queryTx := tx.NewTransaction(fm, lm, bm, lt)
	plan, err := qp.CreatePlan(queryData, queryTx)
	require.NoError(t, err)

	s, err := plan.Open()
	require.NoError(t, err)
	defer s.Close()

	results := make(map[string]int)
	require.NoError(t, s.BeforeFirst())
	for {
		hasNext, err := s.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		dept, err := s.GetString("dept")
		require.NoError(t, err)
		avgSalary, err := s.GetInt("avgOfsalary")
		require.NoError(t, err)

		results[dept] = avgSalary
	}

	assert.Equal(t, 85000, results["Engineering"])
	assert.Equal(t, 62500, results["Sales"])
	require.NoError(t, queryTx.Commit())
}

func TestBasicQueryPlanner_GroupByWithHaving(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	txn := tx.NewTransaction(fm, lm, bm, lt)

	mdm := createTableMetadataWithSchema(t, txn, "sales", map[string]interface{}{
		"product": "string",
		"region":  "string",
		"amount":  0,
	})

	insertTestData(t, txn, "sales", mdm, []map[string]interface{}{
		{"product": "Widget", "region": "North", "amount": 100},
		{"product": "Widget", "region": "North", "amount": 150},
		{"product": "Gadget", "region": "South", "amount": 50},
		{"product": "Gadget", "region": "South", "amount": 75},
	})

	require.NoError(t, txn.Commit())

	qp := NewBasicQueryPlanner(mdm)

	sql := `
		select product, sum(amount) 
		from sales 
		group by product 
		having sum(amount) > 200
	`
	parser := parse.NewParser(sql)
	queryData, err := parser.Query()
	require.NoError(t, err)

	queryTx := tx.NewTransaction(fm, lm, bm, lt)
	plan, err := qp.CreatePlan(queryData, queryTx)
	require.NoError(t, err)

	s, err := plan.Open()
	require.NoError(t, err)
	defer s.Close()

	count := 0
	require.NoError(t, s.BeforeFirst())
	for {
		hasNext, err := s.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++

		product, err := s.GetString("product")
		require.NoError(t, err)
		total, err := s.GetInt("sumOfamount")
		require.NoError(t, err)

		assert.Equal(t, "Widget", product)
		assert.Equal(t, 250, total)
	}
	assert.Equal(t, 1, count)
	require.NoError(t, queryTx.Commit())
}

func TestBasicQueryPlanner_OrderBy(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	txn := tx.NewTransaction(fm, lm, bm, lt)

	mdm := createTableMetadataWithSchema(t, txn, "students", map[string]interface{}{
		"id":    0,
		"name":  "string",
		"grade": 0,
	})

	insertTestData(t, txn, "students", mdm, []map[string]interface{}{
		{"id": 1, "name": "Charlie", "grade": 85},
		{"id": 2, "name": "Alice", "grade": 92},
		{"id": 3, "name": "Bob", "grade": 78},
	})

	require.NoError(t, txn.Commit())

	qp := NewBasicQueryPlanner(mdm)

	sql := `
		select name, grade 
		from students 
		order by grade asc
	`
	parser := parse.NewParser(sql)
	queryData, err := parser.Query()
	require.NoError(t, err)

	queryTx := tx.NewTransaction(fm, lm, bm, lt)
	plan, err := qp.CreatePlan(queryData, queryTx)
	require.NoError(t, err)

	s, err := plan.Open()
	require.NoError(t, err)
	defer s.Close()

	expected := []struct {
		name  string
		grade int
	}{
		{"Bob", 78},
		{"Charlie", 85},
		{"Alice", 92},
	}

	require.NoError(t, s.BeforeFirst())
	idx := 0
	for {
		hasNext, err := s.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		name, err := s.GetString("name")
		require.NoError(t, err)
		grade, err := s.GetInt("grade")
		require.NoError(t, err)

		assert.Equal(t, expected[idx].name, name)
		assert.Equal(t, expected[idx].grade, grade)
		idx++
	}
	assert.Equal(t, len(expected), idx)
	require.NoError(t, queryTx.Commit())
}

func TestBasicQueryPlanner_ComplexQuery(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	txn := tx.NewTransaction(fm, lm, bm, lt)

	mdm := createTableMetadataWithSchema(t, txn, "orders", map[string]interface{}{
		"id":       0,
		"product":  "string",
		"category": "string",
		"amount":   0,
		"date":     "string",
	})

	insertTestData(t, txn, "orders", mdm, []map[string]interface{}{
		{"id": 1, "product": "Laptop", "category": "Electronics", "amount": 2000, "date": "2024-01"},
		{"id": 2, "product": "Phone", "category": "Electronics", "amount": 2400, "date": "2024-01"},
		{"id": 3, "product": "Desk", "category": "Furniture", "amount": 300, "date": "2024-01"},
		{"id": 4, "product": "Laptop", "category": "Electronics", "amount": 1200, "date": "2024-02"},
		{"id": 5, "product": "Chair", "category": "Furniture", "amount": 400, "date": "2024-02"},
	})

	require.NoError(t, txn.Commit())

	qp := NewBasicQueryPlanner(mdm)

	sql := `
		select category, date, sum(amount)
		from orders
		where amount > 500
		group by category, date
		having sum(amount) > 2000
		order by total desc
	`
	parser := parse.NewParser(sql)
	queryData, err := parser.Query()
	require.NoError(t, err)

	queryTx := tx.NewTransaction(fm, lm, bm, lt)
	plan, err := qp.CreatePlan(queryData, queryTx)
	require.NoError(t, err)

	s, err := plan.Open()
	require.NoError(t, err)
	defer s.Close()

	expected := []struct {
		category string
		date     string
		total    int
	}{
		{"Electronics", "2024-01", 4400},
	}

	require.NoError(t, s.BeforeFirst())
	count := 0
	for {
		hasNext, err := s.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		category, err := s.GetString("category")
		require.NoError(t, err)
		date, err := s.GetString("date")
		require.NoError(t, err)
		total, err := s.GetInt("sumOfamount")
		require.NoError(t, err)

		assert.Equal(t, expected[count].category, category)
		assert.Equal(t, expected[count].date, date)
		assert.Equal(t, expected[count].total, total)
		count++
	}

	assert.Equal(t, 1, count)
	require.NoError(t, queryTx.Commit())
}
