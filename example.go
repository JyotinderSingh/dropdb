package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/JyotinderSingh/dropdb/driver" // Import the driver for side effects
)

func main() {
	// Specify the directory for DropDB database files
	dbDir := "./mydb"
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			log.Fatalf("Failed to clean up database directory: %v\n", err)
		}
	}()

	// Open a connection to DropDB
	db, err := sql.Open("dropdb", dbDir)
	if err != nil {
		log.Fatalf("Failed to open DropDB: %v\n", err)
	}
	defer db.Close()

	// ----------------------------------------------------------------
	// 1. Create a table (auto-commit mode)
	// ----------------------------------------------------------------
	fmt.Println("Creating table in auto-commit mode...")
	createTableSQL := `
        CREATE TABLE student (
            sname VARCHAR(10),
            gradyear INT
        )
    `
	if _, err = db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v\n", err)
	}
	fmt.Println("Table 'student' created successfully.\n")

	// ----------------------------------------------------------------
	// 2. Demonstrate a ROLLBACK
	// ----------------------------------------------------------------
	fmt.Println("Starting an explicit transaction and rolling back...")
	tx1, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction tx1: %v\n", err)
	}

	// Insert a row that we'll never commit
	_, err = tx1.Exec(`INSERT INTO student (sname, gradyear) VALUES ('Zoe', 9999)`)
	if err != nil {
		// If any error occurs, roll back and exit
		_ = tx1.Rollback()
		log.Fatalf("Failed to insert in tx1: %v\n", err)
	}

	// Now intentionally rollback
	if err := tx1.Rollback(); err != nil {
		log.Fatalf("Failed to roll back tx1: %v\n", err)
	}

	fmt.Println("Rolled back transaction. Row for 'Zoe' should NOT be in the table.\n")

	// ----------------------------------------------------------------
	// 3. Demonstrate a COMMIT with multiple inserts
	// ----------------------------------------------------------------
	fmt.Println("Starting a second explicit transaction and committing...")
	tx2, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction tx2: %v\n", err)
	}

	// Insert rows into the table inside tx2
	insertStatements := []string{
		`INSERT INTO student (sname, gradyear) VALUES ('Alice', 2023)`,
		`INSERT INTO student (sname, gradyear) VALUES ('Bob', 2024)`,
		`INSERT INTO student (sname, gradyear) VALUES ('Charlie', 2025)`,
	}

	for _, stmt := range insertStatements {
		if _, err := tx2.Exec(stmt); err != nil {
			// If insert fails, roll back
			_ = tx2.Rollback()
			log.Fatalf("Failed to insert row in tx2: %v\n", err)
		}
	}

	// Commit tx2 to persist the inserts
	if err := tx2.Commit(); err != nil {
		log.Fatalf("Failed to commit tx2: %v\n", err)
	}
	fmt.Println("Transaction tx2 committed successfully.\n")

	// ----------------------------------------------------------------
	// 4. Query the table to confirm the results
	// ----------------------------------------------------------------
	fmt.Println("Querying rows...")
	querySQL := "SELECT sname, gradyear FROM student ORDER BY gradyear"
	rows, err := db.Query(querySQL)
	if err != nil {
		log.Fatalf("Failed to query rows: %v\n", err)
	}
	defer rows.Close()

	fmt.Println("Query results:")
	for rows.Next() {
		var name string
		var year int
		// NOTE: The order of columns is reversed due to how SortPlan is implemented. This is a known issue and should be fixed using some changes in the SortPlan implementation.
		if err := rows.Scan(&year, &name); err != nil {
			log.Fatalf("Failed to scan row: %v\n", err)
		}
		fmt.Printf("  - Name: %s, Graduation Year: %d\n", name, year)
	}

	// Check for any errors encountered during iteration
	if err := rows.Err(); err != nil {
		log.Fatalf("Rows iteration error: %v\n", err)
	}

	fmt.Println("\nQuery completed successfully. Notice that 'Zoe' is missing because her insert was rolled back, but 'Alice', 'Bob', and 'Charlie' are present.")
}
