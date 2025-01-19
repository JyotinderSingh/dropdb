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
		err := rows.Scan(&name, &year)
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
