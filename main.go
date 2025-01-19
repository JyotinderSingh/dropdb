package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/JyotinderSingh/dropdb/dropdbdriver" // Import the driver for side effects
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

	// Create a table
	fmt.Println("Creating table...")
	createTableSQL := `
        CREATE TABLE student (
            sname VARCHAR(10),
            gradyear INT
        )
    `
	if _, err = db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v\n", err)
	}
	fmt.Println("Table 'student' created successfully.")

	// Insert rows into the table
	fmt.Println("Inserting rows...")
	insertStatements := []string{
		`INSERT INTO student (sname, gradyear) VALUES ('Alice', 2023)`,
		`INSERT INTO student (sname, gradyear) VALUES ('Bob', 2024)`,
		`INSERT INTO student (sname, gradyear) VALUES ('Charlie', 2025)`,
	}

	for _, stmt := range insertStatements {
		if _, err = db.Exec(stmt); err != nil {
			log.Fatalf("Failed to insert row: %v\n", err)
		}
	}
	fmt.Println("Rows inserted successfully.")

	// Query the table
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
		if err := rows.Scan(&name, &year); err != nil {
			log.Fatalf("Failed to scan row: %v\n", err)
		}
		fmt.Printf("  - Name: %s, Graduation Year: %d\n", name, year)
	}

	// Check for any errors encountered during iteration
	if err := rows.Err(); err != nil {
		log.Fatalf("Rows iteration error: %v\n", err)
	}
	fmt.Println("Query completed successfully.")
}
