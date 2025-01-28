package driver

import (
	"database/sql/driver"
	"fmt"
	"github.com/JyotinderSingh/dropdb/tx"
	"strings"
)

// DropDBStmt implements driver.Stmt.
type DropDBStmt struct {
	conn  *DropDBConn
	query string
}

// Close is a no-op for this simple driver.
func (s *DropDBStmt) Close() error {
	return nil
}

// NumInput returns -1 indicating we don't do bound parameters in this example.
func (s *DropDBStmt) NumInput() int {
	return -1
}

// Exec executes a non-SELECT statement (INSERT, UPDATE, DELETE, CREATE, etc).
// If the statement is actually a SELECT, we throw an error or ignore.
func (s *DropDBStmt) Exec(args []driver.Value) (driver.Result, error) {
	var t *tx.Transaction
	if s.conn.activeTx == nil {
		// create transaction for auto-commit
		t = s.conn.db.NewTx()
	} else {
		// use the existing transaction
		t = s.conn.activeTx
	}

	planner := s.conn.db.Planner()

	// Simple detection if it's a "SELECT" (for a real driver, you'd parse properly).
	lower := strings.ToLower(strings.TrimSpace(s.query))
	if strings.HasPrefix(lower, "select") {
		// By the tests’ logic, Exec() is for CREATE/INSERT/UPDATE/DELETE.
		// You could either:
		//   1. Return an error, or
		//   2. Forward to Query() if you prefer
		return nil, fmt.Errorf("Exec called with SELECT statement: %s", s.query)
	}

	// For all other statements (CREATE, INSERT, UPDATE, DELETE, etc.),
	// use planner.ExecuteUpdate
	rowsAffected, err := planner.ExecuteUpdate(s.query, t)

	if err != nil {
		// if it was an auto-commit transaction, rollback
		if s.conn.activeTx == nil {
			_ = t.Rollback()
		}
		return nil, err
	}

	if s.conn.activeTx == nil {
		// auto-commit
		if err := t.Commit(); err != nil {
			return nil, err
		}
	}

	// Return a driver.Result containing rows-affected count
	return &DropDBResult{rowsAffected: int64(rowsAffected)}, nil
}

// Query executes a SELECT statement and returns the resulting rows.
func (s *DropDBStmt) Query(args []driver.Value) (driver.Rows, error) {
	// Decide whether we're in an explicit transaction or need to auto-commit
	var t *tx.Transaction
	if s.conn.activeTx == nil {
		// No active transaction => create a new one for auto-commit
		t = s.conn.db.NewTx()
	} else {
		// We already have an open transaction
		t = s.conn.activeTx
	}

	// We'll detect SELECT queries by prefix:
	lower := strings.ToLower(strings.TrimSpace(s.query))
	if !strings.HasPrefix(lower, "select") {
		// By the test logic, Query is only for SELECT statements.
		// For everything else (CREATE, INSERT, etc.) we do Exec.
		return nil, fmt.Errorf("Query called with non-SELECT statement: %s", s.query)
	}

	planner := s.conn.db.Planner()

	// Use the Planner to build a query plan
	plan, err := planner.CreateQueryPlan(s.query, t)
	if err != nil {
		// Roll back on error
		_ = t.Rollback()
		return nil, err
	}

	sc, err := plan.Open()
	if err != nil {
		if s.conn.activeTx == nil {
			_ = t.Rollback()
		}
		return nil, err
	}

	// Return the Rows object. We'll commit/rollback inside Rows.Close()
	// (or when the result set is exhausted).
	return &DropDBRows{
		stmt: s,
		tx:   t,
		scan: sc,
		plan: plan,
	}, nil
}
