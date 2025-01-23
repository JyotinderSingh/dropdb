package parse

import (
	"github.com/JyotinderSingh/dropdb/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test a simple SELECT statement with multiple fields, one table, and a WHERE clause.
func TestParserSelect(t *testing.T) {
	sql := "SELECT name, age FROM users WHERE age >= 18 AND name = 'Alice'"
	p := NewParser(sql)

	qd, err := p.Query()
	require.NoError(t, err)

	// Check that we have the expected fields.
	assert.Equal(t, []string{"name", "age"}, qd.Fields())

	// Check that we have the expected tables.
	assert.Equal(t, []string{"users"}, qd.Tables())

	// Check the resulting predicate string or structure.
	predStr := qd.Pred().String()
	// Example result might be: "age>=18 and name='Alice'"
	assert.Contains(t, predStr, "age >= 18")
	assert.Contains(t, predStr, "name = Alice")
}

// Test INSERT statement with different constant types (string, int, bool, date).
func TestParserInsert(t *testing.T) {
	// Note we are supporting a date in the format YYYY-MM-DD
	sql := "INSERT INTO people (name, birthdate, is_active, score) VALUES ('Bob', 2025-01-02, true, 42)"
	p := NewParser(sql)

	cmd, err := p.UpdateCmd()
	require.NoError(t, err)

	insertData, ok := cmd.(*InsertData)
	require.True(t, ok)

	assert.Equal(t, "people", insertData.TableName())
	assert.Equal(t, []string{"name", "birthdate", "is_active", "score"}, insertData.Fields())

	// We expect 4 values in the same order as above
	require.Len(t, insertData.Values(), 4)

	// 1) name -> string
	assert.Equal(t, "Bob", insertData.Values()[0])

	// 2) birthdate -> time.Time (the lexer + parser parse it as a date)
	birthdateVal := insertData.Values()[1]
	dateVal, dateOK := birthdateVal.(time.Time)
	require.True(t, dateOK)
	// Check the actual date
	assert.Equal(t, time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), dateVal)

	// 3) is_active -> bool
	boolVal, boolOK := insertData.Values()[2].(bool)
	require.True(t, boolOK)
	assert.True(t, boolVal)

	// 4) score -> int
	scoreVal, scoreOK := insertData.Values()[3].(int)
	require.True(t, scoreOK)
	assert.Equal(t, 42, scoreVal)
}

// Test DELETE statement with an optional WHERE clause.
func TestParserDelete(t *testing.T) {
	sql := "DELETE FROM employees WHERE role='Manager' AND salary >= 90000"
	p := NewParser(sql)

	cmd, err := p.UpdateCmd()
	require.NoError(t, err)
	deleteData, ok := cmd.(*DeleteData)
	require.True(t, ok)

	assert.Equal(t, "employees", deleteData.TableName())

	// Check the predicate
	predStr := deleteData.Predicate().String()
	assert.Contains(t, predStr, "role = Manager")
	assert.Contains(t, predStr, "salary >= 90000")
}

// Test UPDATE statement with a single "set" and optional WHERE.
func TestParserUpdate(t *testing.T) {
	sql := "UPDATE projects SET status = 'Completed' WHERE end_date <= 2025-12-31"
	p := NewParser(sql)

	cmd, err := p.UpdateCmd()
	require.NoError(t, err)
	modData, ok := cmd.(*ModifyData)
	require.True(t, ok)

	assert.Equal(t, "projects", modData.TableName())
	assert.Equal(t, "status", modData.fieldName)
	assert.Equal(t, "Completed", modData.NewValue().String())

	// Check the predicate
	predStr := modData.Predicate().String()
	assert.Contains(t, predStr, "end_date <= 2025-12-31")
}

// Test CREATE TABLE statement with multiple columns (int, varchar, bool, date).
func TestParserCreateTable(t *testing.T) {
	sql := `
CREATE TABLE tasks (
    id int,
    description varchar(50),
    is_done bool,
    due_date date
)
`
	p := NewParser(sql)

	cmd, err := p.UpdateCmd()
	require.NoError(t, err)

	tableData, ok := cmd.(*CreateTableData)
	require.True(t, ok)

	assert.Equal(t, "tasks", tableData.TableName())

	sch := tableData.NewSchema()
	assert.Equal(t, 4, len(sch.Fields()))
	assert.True(t, sch.HasField("id"))
	assert.Equal(t, types.Integer, sch.Type("id"))
	assert.True(t, sch.HasField("description"))
	assert.Equal(t, types.Varchar, sch.Type("description"))
	assert.True(t, sch.HasField("is_done"))
	assert.Equal(t, types.Boolean, sch.Type("is_done"))
	assert.True(t, sch.HasField("due_date"))
	assert.Equal(t, types.Date, sch.Type("due_date"))
}

// Test CREATE VIEW statement with a query inside.
func TestParserCreateView(t *testing.T) {
	sql := "CREATE VIEW active_users AS SELECT name, last_login FROM users WHERE is_active=true"
	p := NewParser(sql)

	cmd, err := p.UpdateCmd()
	require.NoError(t, err)

	viewData, ok := cmd.(*CreateViewData)
	require.True(t, ok)

	assert.Equal(t, "active_users", viewData.ViewName())

	// The QueryData is inside. We can do a string check:
	viewDef := viewData.ViewDefinition()
	assert.Contains(t, viewDef, "select name, last_login from users where is_active = true")
}

// Test CREATE INDEX statement with single field.
func TestParserCreateIndex(t *testing.T) {
	sql := "CREATE INDEX idx_name ON people(name)"
	p := NewParser(sql)

	cmd, err := p.UpdateCmd()
	require.NoError(t, err)

	indexData, ok := cmd.(*CreateIndexData)
	require.True(t, ok)

	assert.Equal(t, "idx_name", indexData.IndexName())
	assert.Equal(t, "people", indexData.TableName())
	assert.Equal(t, "name", indexData.FieldName())
}

// Test for invalid syntax to ensure we return an error.
func TestParserInvalidSyntax(t *testing.T) {
	sql := "SELECT FROM" // Missing field(s)
	p := NewParser(sql)

	_, err := p.Query()
	require.Error(t, err, "expected an error for invalid syntax")

	assert.Contains(t, err.Error(), "syntax")
}
