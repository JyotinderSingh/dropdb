package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/record"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/parse"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
)

func createNewMetadataManager(t *testing.T, fm *file.Manager, lm *log.Manager, bm *buffer.Manager, lt *concurrency.LockTable) *metadata.Manager {
	txn := tx.NewTransaction(fm, lm, bm, lt)

	mdm, err := metadata.NewManager(true, txn)
	require.NoError(t, err, "failed to create metadata.Manager")
	require.NoError(t, txn.Commit())
	return mdm
}

// A helper that runs a query (using BasicQueryPlanner or any other) and returns rows as a slice of map.
func runQuery(t *testing.T, mdm *metadata.Manager, sql string, fm *file.Manager, lm *log.Manager, bm *buffer.Manager, lt *concurrency.LockTable) []map[string]any {
	// We create a new transaction for the query.
	txn := tx.NewTransaction(fm, lm, bm, lt)
	defer func() { require.NoError(t, txn.Commit()) }()

	// Use or create a BasicQueryPlanner to plan the query.
	qp := NewBasicQueryPlanner(mdm)

	parser := parse.NewParser(sql)
	qd, err := parser.Query()
	require.NoError(t, err, "query parse error")

	plan, err := qp.CreatePlan(qd, txn)
	require.NoError(t, err, "failed to create query plan")

	s, err := plan.Open()
	require.NoError(t, err, "failed to open scan")
	defer s.Close()

	require.NoError(t, s.BeforeFirst())

	var results []map[string]any
	for {
		hasNext, err := s.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		// Collect all fields from the QueryData
		row := make(map[string]any)
		for _, fld := range qd.Fields() {
			switch sVal, err := s.GetVal(fld); val := sVal.(type) {
			case int:
				row[fld] = val
			case string:
				row[fld] = val
			case bool:
				row[fld] = val
			case time.Time:
				row[fld] = val
			default:
				if err != nil {
					t.Fatalf("error reading field %s: %v", fld, err)
				}
				row[fld] = sVal // fallback
			}
		}
		results = append(results, row)
	}
	return results
}

func TestBasicUpdatePlanner_CreateInsertSelect(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	mdm := createNewMetadataManager(t, fm, lm, bm, lt)

	// Create the BasicUpdatePlanner
	up := NewBasicUpdatePlanner(mdm)

	// 1) CREATE TABLE
	// We'll manually build the parse.CreateTableData, or parse an actual SQL:
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	// Equivalent to "CREATE TABLE people (id int, name varchar(20))"
	ctd := parse.NewCreateTableData("people", schema)
	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := up.ExecuteCreateTable(ctd, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// 2) INSERT a row into that table
	// "INSERT INTO people (id, name) VALUES (1, 'Alice')"
	insData := parse.NewInsertData(
		"people",
		[]string{"id", "name"},
		[]any{1, "Alice"},
	)
	txn2 := tx.NewTransaction(fm, lm, bm, lt)
	cnt, err := up.ExecuteInsert(insData, txn2)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "should have inserted 1 row")
	require.NoError(t, txn2.Commit())

	// 3) SELECT the row back to verify
	rows := runQuery(t, mdm, "select id, name from people", fm, lm, bm, lt)
	require.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, "Alice", rows[0]["name"])
}

func TestBasicUpdatePlanner_Modify(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	mdm := createNewMetadataManager(t, fm, lm, bm, lt)
	up := NewBasicUpdatePlanner(mdm)

	// CREATE TABLE "users" (id int, age int)
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("age")
	ctd := parse.NewCreateTableData("users", schema)

	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := up.ExecuteCreateTable(ctd, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Insert some rows
	rowsToInsert := []map[string]any{
		{"id": 1, "age": 20},
		{"id": 2, "age": 30},
		{"id": 3, "age": 40},
	}
	for _, row := range rowsToInsert {
		ins := parse.NewInsertData("users",
			[]string{"id", "age"},
			[]any{row["id"], row["age"]})
		txnIns := tx.NewTransaction(fm, lm, bm, lt)
		_, err := up.ExecuteInsert(ins, txnIns)
		require.NoError(t, err)
		require.NoError(t, txnIns.Commit())
	}

	predGe30 := query.NewPredicateFromTerm(
		query.NewTerm(
			query.NewFieldExpression("age"),
			query.NewConstantExpression(30),
			query.GE,
		),
	)

	modData := parse.NewModifyData("users", "age", query.NewConstantExpression(60), predGe30)

	txnMod := tx.NewTransaction(fm, lm, bm, lt)
	updatedCount, err := up.ExecuteModify(modData, txnMod)
	require.NoError(t, err)
	require.NoError(t, txnMod.Commit())

	// 2 rows have age >= 30, so updatedCount should be 2
	assert.Equal(t, 2, updatedCount)

	// Check results
	rows := runQuery(t, mdm, "select id, age from users order by id", fm, lm, bm, lt)
	require.Len(t, rows, 3)

	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, 20, rows[0]["age"])
	assert.Equal(t, 2, rows[1]["id"])
	assert.Equal(t, 60, rows[1]["age"])
	assert.Equal(t, 3, rows[2]["id"])
	assert.Equal(t, 60, rows[2]["age"])
}

func TestBasicUpdatePlanner_Delete(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	mdm := createNewMetadataManager(t, fm, lm, bm, lt)
	up := NewBasicUpdatePlanner(mdm)

	// CREATE TABLE "temp" (val int)
	schema := record.NewSchema()
	schema.AddIntField("val")
	ctd := parse.NewCreateTableData("temp", schema)

	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := up.ExecuteCreateTable(ctd, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Insert rows: val=1, val=2, val=3
	for i := 1; i <= 3; i++ {
		ins := parse.NewInsertData("temp", []string{"val"}, []any{i})
		txnIns := tx.NewTransaction(fm, lm, bm, lt)
		_, err := up.ExecuteInsert(ins, txnIns)
		require.NoError(t, err)
		require.NoError(t, txnIns.Commit())
	}

	// 3 rows inserted
	rows := runQuery(t, mdm, "select val from temp", fm, lm, bm, lt)
	require.Len(t, rows, 3)

	// Now we delete rows where val >= 2
	// parse.DeleteData(table="temp", pred="val>=2")
	pred := query.NewPredicateFromTerm(query.NewTerm(
		query.NewFieldExpression("val"),
		query.NewConstantExpression(2),
		query.GE,
	))
	delData := parse.NewDeleteData("temp", pred)

	txnDel := tx.NewTransaction(fm, lm, bm, lt)
	deletedCount, err := up.ExecuteDelete(delData, txnDel)
	require.NoError(t, err)
	require.NoError(t, txnDel.Commit())

	// 2 rows matched (val=2, val=3)
	assert.Equal(t, 2, deletedCount)

	// Now only val=1 remains
	rows = runQuery(t, mdm, "select val from temp", fm, lm, bm, lt)
	require.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["val"])
}

func TestBasicUpdatePlanner_CreateView(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	mdm := createNewMetadataManager(t, fm, lm, bm, lt)
	up := NewBasicUpdatePlanner(mdm)

	cvd := parse.NewCreateViewData("myview", parse.NewQueryData([]string{"dummy"}, []string{"dual"}, query.NewPredicate()))

	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := up.ExecuteCreateView(cvd, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Check that the metadata manager recorded "myview"
	viewDef, err := mdm.GetViewDefinition("myview", txn)
	require.NoError(t, err)
	assert.Equal(t, "select dummy from dual", viewDef)
}

func TestBasicUpdatePlanner_CreateIndex(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	mdm := createNewMetadataManager(t, fm, lm, bm, lt)
	up := NewBasicUpdatePlanner(mdm)

	// 1) Create a test table
	schema := record.NewSchema()
	schema.AddIntField("user_id")
	ctd := parse.NewCreateTableData("users", schema)
	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := up.ExecuteCreateTable(ctd, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// 2) Create an index on "user_id"
	cid := parse.NewCreateIndexData("idx_user_id", "users", "user_id")
	txn2 := tx.NewTransaction(fm, lm, bm, lt)
	_, err = up.ExecuteCreateIndex(cid, txn2)
	require.NoError(t, err)
	require.NoError(t, txn2.Commit())

	// Check that the index was created
	idxInfo, err := mdm.GetIndexInfo("users", txn)
	require.NoError(t, err)
	require.Contains(t, idxInfo, "user_id")
}
