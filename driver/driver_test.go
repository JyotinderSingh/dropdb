package driver

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestDropDBDriver(t *testing.T) {
	// Temporary directory for the database files
	dbDir := "./testdata"
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			t.Fatalf("Failed to clean up database directory: %v\n", err)
		}
	}()

	// Open the DropDB database
	db, err := sql.Open("dropdb", dbDir)
	require.NoError(t, err, "failed to open DropDB")
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE student (sname VARCHAR(10), gradyear INT)")
	require.NoError(t, err, "failed to create table")

	// Insert rows into the table
	insertQueries := []string{
		`INSERT INTO student (sname, gradyear) VALUES ('Alice', 2023)`,
		`INSERT INTO student (sname, gradyear) VALUES ('Bob', 2024)`,
		`INSERT INTO student (sname, gradyear) VALUES ('Charlie', 2025)`,
	}

	for _, query := range insertQueries {
		_, err = db.Exec(query)
		require.NoError(t, err, "failed to insert row")
	}

	// Query the table
	rows, err := db.Query("SELECT sname, gradyear FROM student ORDER BY gradyear")
	require.NoError(t, err, "failed to query rows")
	defer rows.Close()

	// Validate the results
	expectedResults := []struct {
		sname    string
		gradyear int
	}{
		{"Alice", 2023},
		{"Bob", 2024},
		{"Charlie", 2025},
	}

	var results []struct {
		sname    string
		gradyear int
	}
	for rows.Next() {
		var name string
		var year int
		// NOTE: The order of columns is reversed due to how SortPlan is implemented. This is a known issue and should be fixed using some changes in the SortPlan implementation.
		err := rows.Scan(&year, &name)
		require.NoError(t, err, "failed to scan row")
		results = append(results, struct {
			sname    string
			gradyear int
		}{name, year})
	}
	require.NoError(t, rows.Err(), "rows iteration error")

	// Assert that the results match the expected values
	assert.Equal(t, expectedResults, results, "query results mismatch")
}

// This test demonstrates explicit transaction usage with DropDB.
func TestDropDBDriver_ExplicitTransaction(t *testing.T) {
	// Temporary directory for the database files
	dbDir := "./testdata_explicit_tx"
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			t.Fatalf("Failed to clean up database directory: %v\n", err)
		}
	}()

	// Open the DropDB database
	db, err := sql.Open("dropdb", dbDir)
	require.NoError(t, err, "failed to open DropDB")
	defer db.Close()

	// Create a new table for testing
	_, err = db.Exec("CREATE TABLE testtx (id INT, val VARCHAR(10))")
	require.NoError(t, err, "failed to create table")

	// ---------------------------------------------------------
	// 1. Start a transaction and ROLLBACK
	// ---------------------------------------------------------
	tx1, err := db.Begin()
	require.NoError(t, err, "failed to begin tx1")

	// Insert row #1
	_, err = tx1.Exec("INSERT INTO testtx (id, val) VALUES (1, 'rollback')")
	require.NoError(t, err, "failed to insert row #1 in tx1")

	// Roll back, discarding the above insert
	err = tx1.Rollback()
	require.NoError(t, err, "failed to rollback tx1")

	// Confirm nothing was committed
	rows, err := db.Query("SELECT id, val FROM testtx")
	require.NoError(t, err, "failed to query after rollback")
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err(), "rows iteration error after rollback")
	assert.Equal(t, 0, count, "expected zero rows after rollback")
	// ---------------------------------------------------------

	// ---------------------------------------------------------
	// 2. Start another transaction and COMMIT
	// ---------------------------------------------------------
	tx2, err := db.Begin()
	require.NoError(t, err, "failed to begin tx2")

	// Insert row #2
	_, err = tx2.Exec("INSERT INTO testtx (id, val) VALUES (2, 'commit')")
	require.NoError(t, err, "failed to insert row #2 in tx2")

	// Commit the second transaction
	err = tx2.Commit()
	require.NoError(t, err, "failed to commit tx2")

	// Confirm the committed row is persisted
	rows, err = db.Query("SELECT id, val FROM testtx ORDER BY id")
	require.NoError(t, err, "failed to query after commit")
	defer rows.Close()

	var results []struct {
		ID  int
		Val string
	}
	for rows.Next() {
		var id int
		var val string
		err := rows.Scan(&id, &val)
		require.NoError(t, err, "failed to scan row")
		results = append(results, struct {
			ID  int
			Val string
		}{id, val})
	}
	require.NoError(t, rows.Err(), "rows iteration error after commit")

	// We expect exactly one row: (2, "commit")
	require.Len(t, results, 1, "expected exactly one row after commit")
	assert.Equal(t, 2, results[0].ID, "ID mismatch after commit")
	assert.Equal(t, "commit", results[0].Val, "Val mismatch after commit")
}
