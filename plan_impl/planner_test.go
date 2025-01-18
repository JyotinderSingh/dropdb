package plan_impl

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
)

func setupPlannerTest(t *testing.T, blockSize, numBuffers int) (*Planner, *metadata.Manager, *file.Manager, *log.Manager, *buffer.Manager, *concurrency.LockTable) {
	// Reuse the helper that sets up file/log/buffer managers
	dbDir := t.TempDir()
	fm, err := file.NewManager(dbDir, blockSize)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, numBuffers)
	lt := concurrency.NewLockTable()

	// Create a brand new metadata manager
	txn := tx.NewTransaction(fm, lm, bm, lt)
	mdm, err := metadata.NewManager(true, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Build the lower-level planners
	qp := NewBasicQueryPlanner(mdm)
	up := NewBasicUpdatePlanner(mdm)

	// Wrap them in the encapsulating Planner
	p := NewPlanner(qp, up)
	return p, mdm, fm, lm, bm, lt
}

// runPlannerQuery is a helper to run a SELECT statement via Planner.CreateQueryPlan
// and return all rows as a slice of maps.
func runPlannerQuery(
	t *testing.T,
	p *Planner,
	sql string,
	fm *file.Manager,
	lm *log.Manager,
	bm *buffer.Manager,
	lt *concurrency.LockTable,
	fields []string,
) []map[string]any {
	txn := tx.NewTransaction(fm, lm, bm, lt)
	defer func() { require.NoError(t, txn.Commit()) }()

	plan, err := p.CreateQueryPlan(sql, txn)
	require.NoError(t, err, "failed to create query plan for: %s", sql)

	s, err := plan.Open()
	require.NoError(t, err, "failed to open scan for: %s", sql)
	defer s.Close()

	require.NoError(t, s.BeforeFirst())

	var results []map[string]any
	for {
		hasNext, err := s.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		row := make(map[string]any)
		for _, fld := range fields {
			val, err := s.GetVal(fld)
			require.NoError(t, err)

			switch converted := val.(type) {
			case int:
				row[fld] = converted
			case string:
				row[fld] = converted
			case bool:
				row[fld] = converted
			case time.Time:
				row[fld] = converted
			default:
				// If your engine supports other types, handle them here
				row[fld] = val
			}
		}
		results = append(results, row)
	}
	return results
}

func TestPlanner_CreateTableInsertSelect(t *testing.T) {
	// 1) Setup
	p, _, fm, lm, bm, lt := setupPlannerTest(t, 8800, 8)

	// 2) Create table using the Planner's ExecuteUpdate
	createTableSQL := `
        CREATE TABLE people (id INT, name VARCHAR(20))
    `
	txn := tx.NewTransaction(fm, lm, bm, lt)
	updatedCount, err := p.ExecuteUpdate(createTableSQL, txn)
	require.NoError(t, err)
	// CREATE statements typically return 0
	assert.Equal(t, 0, updatedCount)
	require.NoError(t, txn.Commit())

	// 3) Insert a row
	insertSQL := `
        INSERT INTO people (id, name) VALUES (1, 'Alice')
    `
	txnIns := tx.NewTransaction(fm, lm, bm, lt)
	updatedCount, err = p.ExecuteUpdate(insertSQL, txnIns)
	require.NoError(t, err)
	// 1 row inserted
	assert.Equal(t, 1, updatedCount)
	require.NoError(t, txnIns.Commit())

	// 4) Select to verify
	selectSQL := `SELECT id, name FROM people`
	rows := runPlannerQuery(t, p, selectSQL, fm, lm, bm, lt, []string{"id", "name"})
	require.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, "Alice", rows[0]["name"])
}

func TestPlanner_UpdateModify(t *testing.T) {
	// 1) Setup
	p, _, fm, lm, bm, lt := setupPlannerTest(t, 8800, 8)

	// 2) CREATE TABLE users
	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := p.ExecuteUpdate("CREATE TABLE users (id INT, age INT)", txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// 3) INSERT rows
	for i, age := range []int{20, 30, 40} {
		txnIns := tx.NewTransaction(fm, lm, bm, lt)
		insertSQL := `
            INSERT INTO users (id, age) VALUES (%d, %d)
        `
		_, err := p.ExecuteUpdate(
			fmt.Sprintf(insertSQL, i+1, age),
			txnIns,
		)
		require.NoError(t, err)
		require.NoError(t, txnIns.Commit())
	}

	// 4) UPDATE (modify) rows: "UPDATE users SET age=60 WHERE age>=30"
	txnMod := tx.NewTransaction(fm, lm, bm, lt)
	updateSQL := `
        UPDATE users SET age = 60 WHERE age >= 30
    `
	updatedCount, err := p.ExecuteUpdate(updateSQL, txnMod)
	require.NoError(t, err)
	// 2 rows match age >= 30
	assert.Equal(t, 2, updatedCount)
	require.NoError(t, txnMod.Commit())

	// 5) Check results
	selectSQL := `SELECT id, age FROM users ORDER BY id`
	rows := runPlannerQuery(t, p, selectSQL, fm, lm, bm, lt, []string{"id", "age"})
	// Expect 3 rows total
	//   id=1, age=20
	//   id=2, age=60 (updated)
	//   id=3, age=60 (updated)
	require.Len(t, rows, 3)
	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, 20, rows[0]["age"])
	assert.Equal(t, 2, rows[1]["id"])
	assert.Equal(t, 60, rows[1]["age"])
	assert.Equal(t, 3, rows[2]["id"])
	assert.Equal(t, 60, rows[2]["age"])
}

func TestPlanner_Delete(t *testing.T) {
	// 1) Setup
	p, _, fm, lm, bm, lt := setupPlannerTest(t, 8800, 8)

	// 2) CREATE TABLE
	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := p.ExecuteUpdate("CREATE TABLE temps (val INT)", txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// 3) INSERT some rows: val=1,2,3
	for i := 1; i <= 3; i++ {
		insertSQL := `
            INSERT INTO temps (val) VALUES (%d)
        `
		txnIns := tx.NewTransaction(fm, lm, bm, lt)
		_, err := p.ExecuteUpdate(fmt.Sprintf(insertSQL, i), txnIns)
		require.NoError(t, err)
		require.NoError(t, txnIns.Commit())
	}

	// Confirm they exist
	rows := runPlannerQuery(t, p, "SELECT val FROM temps ORDER BY val", fm, lm, bm, lt, []string{"val"})
	require.Len(t, rows, 3)

	// 4) DELETE "WHERE val >= 2"
	deleteSQL := `
        DELETE FROM temps WHERE val >= 2
    `
	txnDel := tx.NewTransaction(fm, lm, bm, lt)
	deletedCount, err := p.ExecuteUpdate(deleteSQL, txnDel)
	require.NoError(t, err)
	// Should have deleted 2 rows
	assert.Equal(t, 2, deletedCount)
	require.NoError(t, txnDel.Commit())

	// Now only val=1 remains
	rows = runPlannerQuery(t, p, "SELECT val FROM temps ORDER BY val", fm, lm, bm, lt, []string{"val"})
	require.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["val"])
}

func TestPlanner_CreateView(t *testing.T) {
	// 1) Setup
	p, mdm, fm, lm, bm, lt := setupPlannerTest(t, 8800, 8)

	// 2) Create a dummy base table "dual" just to allow a view def
	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := p.ExecuteUpdate("CREATE TABLE dual (dummy INT)", txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// 3) CREATE VIEW using the Planner
	createViewSQL := `
        CREATE VIEW myview AS SELECT dummy FROM dual
    `
	txn2 := tx.NewTransaction(fm, lm, bm, lt)
	updatedCount, err := p.ExecuteUpdate(createViewSQL, txn2)
	require.NoError(t, err)
	// Typically returns 0
	assert.Equal(t, 0, updatedCount)
	require.NoError(t, txn2.Commit())

	// 4) Confirm that the metadata manager sees the view
	txn3 := tx.NewTransaction(fm, lm, bm, lt)
	viewDef, err := mdm.GetViewDefinition("myview", txn3)
	require.NoError(t, err)
	require.NoError(t, txn3.Commit())

	// Should be "select dummy from dual"
	assert.Equal(t, "select dummy from dual", viewDef)
}

func TestPlanner_CreateIndex(t *testing.T) {
	// 1) Setup
	p, mdm, fm, lm, bm, lt := setupPlannerTest(t, 8800, 8)

	// 2) CREATE TABLE
	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := p.ExecuteUpdate("CREATE TABLE users (user_id INT)", txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// 3) CREATE INDEX
	createIndexSQL := `
        CREATE INDEX idx_user_id ON users (user_id)
    `
	txn2 := tx.NewTransaction(fm, lm, bm, lt)
	updatedCount, err := p.ExecuteUpdate(createIndexSQL, txn2)
	require.NoError(t, err)
	assert.Equal(t, 0, updatedCount) // Typically 0
	require.NoError(t, txn2.Commit())

	// 4) Confirm in metadata
	txn3 := tx.NewTransaction(fm, lm, bm, lt)
	idxInfo, err := mdm.GetIndexInfo("users", txn3)
	require.NoError(t, err)
	require.NoError(t, txn3.Commit())

	require.Contains(t, idxInfo, "user_id", "index info should contain an index on user_id")
}
