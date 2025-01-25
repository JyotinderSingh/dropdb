package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/parse"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIndexUpdatePlanner_InsertWithIndex(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	mdm := createNewMetadataManager(t, fm, lm, bm, lt)
	up := NewIndexUpdatePlanner(mdm)

	// Create table with an indexed field
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("email", 50)
	ctd := parse.NewCreateTableData("users", schema)

	txn := tx.NewTransaction(fm, lm, bm, lt)
	_, err := up.ExecuteCreateTable(ctd, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Create index on email
	cid := parse.NewCreateIndexData("idx_email", "users", "email")
	txn = tx.NewTransaction(fm, lm, bm, lt)
	_, err = up.ExecuteCreateIndex(cid, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Insert records
	records := []struct {
		id    int
		email string
	}{
		{1, "alice@test.com"},
		{2, "bob@test.com"},
	}

	for _, r := range records {
		txn = tx.NewTransaction(fm, lm, bm, lt)
		insData := parse.NewInsertData(
			"users",
			[]string{"id", "email"},
			[]any{r.id, r.email},
		)
		cnt, err := up.ExecuteInsert(insData, txn)
		require.NoError(t, err)
		assert.Equal(t, 1, cnt)
		require.NoError(t, txn.Commit())
	}

	// Verify records through index scan
	rows := runQuery(t, mdm, "select id, email from users where email = 'alice@test.com'", fm, lm, bm, lt)
	require.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, "alice@test.com", rows[0]["email"])
}

func TestIndexUpdatePlanner_ModifyWithIndex(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	mdm := createNewMetadataManager(t, fm, lm, bm, lt)
	up := NewIndexUpdatePlanner(mdm)

	// Create table with indexed fields
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("age")
	schema.AddStringField("status", 20)

	txn := tx.NewTransaction(fm, lm, bm, lt)
	ctd := parse.NewCreateTableData("employees", schema)
	_, err := up.ExecuteCreateTable(ctd, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Create index on status
	txn = tx.NewTransaction(fm, lm, bm, lt)
	cid := parse.NewCreateIndexData("idx_status", "employees", "status")
	_, err = up.ExecuteCreateIndex(cid, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Insert test data
	records := []map[string]any{
		{"id": 1, "age": 25, "status": "active"},
		{"id": 2, "age": 30, "status": "active"},
		{"id": 3, "age": 35, "status": "inactive"},
	}

	for _, r := range records {
		txn = tx.NewTransaction(fm, lm, bm, lt)
		insData := parse.NewInsertData(
			"employees",
			[]string{"id", "age", "status"},
			[]any{r["id"], r["age"], r["status"]},
		)
		_, err := up.ExecuteInsert(insData, txn)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	// Modify status where age >= 30
	pred := query.NewPredicateFromTerm(
		query.NewTerm(
			query.NewFieldExpression("age"),
			query.NewConstantExpression(30),
			types.GE,
		),
	)

	txn = tx.NewTransaction(fm, lm, bm, lt)
	modData := parse.NewModifyData(
		"employees",
		"status",
		query.NewConstantExpression("retired"),
		pred,
	)

	count, err := up.ExecuteModify(modData, txn)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	require.NoError(t, txn.Commit())

	// Verify through indexed scan
	rows := runQuery(t, mdm, "select id, status from employees where status = 'retired'", fm, lm, bm, lt)
	require.Len(t, rows, 2)
	assert.Equal(t, "retired", rows[0]["status"])
	assert.Equal(t, "retired", rows[1]["status"])

	// Verify old index entries are removed
	rows = runQuery(t, mdm, "select id from employees where status = 'active'", fm, lm, bm, lt)
	require.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0]["id"])
}

func TestIndexUpdatePlanner_DeleteWithIndex(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	mdm := createNewMetadataManager(t, fm, lm, bm, lt)
	up := NewIndexUpdatePlanner(mdm)

	// Create table with indexed field
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("category", 20)
	schema.AddIntField("count")

	txn := tx.NewTransaction(fm, lm, bm, lt)
	ctd := parse.NewCreateTableData("products", schema)
	_, err := up.ExecuteCreateTable(ctd, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Create index on category
	txn = tx.NewTransaction(fm, lm, bm, lt)
	cid := parse.NewCreateIndexData("idx_category", "products", "category")
	_, err = up.ExecuteCreateIndex(cid, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Insert test data
	records := []map[string]any{
		{"id": 1, "category": "electronics", "count": 10},
		{"id": 2, "category": "books", "count": 20},
		{"id": 3, "category": "electronics", "count": 15},
	}

	for _, r := range records {
		txn = tx.NewTransaction(fm, lm, bm, lt)
		insData := parse.NewInsertData(
			"products",
			[]string{"id", "category", "count"},
			[]any{r["id"], r["category"], r["count"]},
		)
		_, err := up.ExecuteInsert(insData, txn)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	// Delete all electronics
	pred := query.NewPredicateFromTerm(
		query.NewTerm(
			query.NewFieldExpression("category"),
			query.NewConstantExpression("electronics"),
			types.EQ,
		),
	)

	txn = tx.NewTransaction(fm, lm, bm, lt)
	delData := parse.NewDeleteData("products", pred)
	count, err := up.ExecuteDelete(delData, txn)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	require.NoError(t, txn.Commit())

	// Verify through index scan
	rows := runQuery(t, mdm, "select id, category from products where category = 'electronics'", fm, lm, bm, lt)
	assert.Empty(t, rows)

	// Verify remaining records
	rows = runQuery(t, mdm, "select id, category, count from products", fm, lm, bm, lt)
	require.Len(t, rows, 1)
	assert.Equal(t, "books", rows[0]["category"])
	assert.Equal(t, 2, rows[0]["id"])
	assert.Equal(t, 20, rows[0]["count"])
}

func TestIndexUpdatePlanner_MultipleIndices(t *testing.T) {
	fm, lm, bm, lt := setupTestManagers(t, 800, 8)
	mdm := createNewMetadataManager(t, fm, lm, bm, lt)
	up := NewIndexUpdatePlanner(mdm)

	// Create table with multiple indexed fields
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("department", 20)
	schema.AddIntField("salary")

	txn := tx.NewTransaction(fm, lm, bm, lt)
	ctd := parse.NewCreateTableData("employees", schema)
	_, err := up.ExecuteCreateTable(ctd, txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Create indices
	txn = tx.NewTransaction(fm, lm, bm, lt)
	_, err = up.ExecuteCreateIndex(parse.NewCreateIndexData("idx_dept", "employees", "department"), txn)
	require.NoError(t, err)
	_, err = up.ExecuteCreateIndex(parse.NewCreateIndexData("idx_salary", "employees", "salary"), txn)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	// Insert test data
	records := []map[string]any{
		{"id": 1, "department": "IT", "salary": 70000},
		{"id": 2, "department": "HR", "salary": 60000},
		{"id": 3, "department": "IT", "salary": 80000},
	}

	for _, r := range records {
		txn = tx.NewTransaction(fm, lm, bm, lt)
		insData := parse.NewInsertData(
			"employees",
			[]string{"id", "department", "salary"},
			[]any{r["id"], r["department"], r["salary"]},
		)
		_, err := up.ExecuteInsert(insData, txn)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	// Modify salary where department = IT
	pred := query.NewPredicateFromTerm(
		query.NewTerm(
			query.NewFieldExpression("department"),
			query.NewConstantExpression("IT"),
			types.EQ,
		),
	)

	txn = tx.NewTransaction(fm, lm, bm, lt)
	modData := parse.NewModifyData(
		"employees",
		"salary",
		query.NewConstantExpression(90000),
		pred,
	)

	count, err := up.ExecuteModify(modData, txn)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	require.NoError(t, txn.Commit())

	// Verify through both indices
	rows := runQuery(t, mdm, "select id, salary from employees where department = 'IT'", fm, lm, bm, lt)
	require.Len(t, rows, 2)
	assert.Equal(t, 90000, rows[0]["salary"])
	assert.Equal(t, 90000, rows[1]["salary"])

	rows = runQuery(t, mdm, "select id, department from employees where salary = 90000", fm, lm, bm, lt)
	require.Len(t, rows, 2)
	assert.Equal(t, "IT", rows[0]["department"])
	assert.Equal(t, "IT", rows[1]["department"])
}
