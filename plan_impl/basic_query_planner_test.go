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
