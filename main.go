package main

import (
	"database/sql"
	"fmt"
	"github.com/JyotinderSingh/dropdb/dropdbdriver"
	"os"
)

var _ = dropdbdriver.DropDBDriver{} // Ensure the driver is imported
func main() {
	// dbDir is the directory for the .tbl, .log, etc. managed by DropDB
	dbDir := "./mydb"
	defer os.RemoveAll(dbDir)

	db, err := sql.Open("dropdb", dbDir)
	if err != nil {
		fmt.Printf("open error: %v\n", err)
		return
	}
	defer db.Close()

	// Example: create a table
	if _, err = db.Exec("CREATE TABLE student (sname VARCHAR(10), gradyear INT)"); err != nil {
		fmt.Printf("create table error: %v\n", err)
		return
	}

	// Insert some rows
	if _, err = db.Exec(`INSERT INTO student(sname, gradyear) VALUES ('Alice', 2023)`); err != nil {
		fmt.Printf("insert error: %v\n", err)
		return
	}
	if _, err = db.Exec(`INSERT INTO student(sname, gradyear) VALUES ('Bob', 2024)`); err != nil {
		fmt.Printf("insert error: %v\n", err)
		return
	}
	if _, err = db.Exec(`INSERT INTO student(sname, gradyear) VALUES ('Charlie', 2025)`); err != nil {
		fmt.Printf("insert error: %v\n", err)
		return
	}

	// Query
	rows, err := db.Query("SELECT sname, gradyear FROM student")
	if err != nil {
		fmt.Printf("query error: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var year int
		if err := rows.Scan(&name, &year); err != nil {
			fmt.Printf("scan error: %v\n", err)
			return
		}
		fmt.Println("Row:", name, year)
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("rows error: %v\n", err)
		return
	}
}
