package driver

import (
	"database/sql/driver"
	"github.com/JyotinderSingh/dropdb/server"
)

// DropDBConn implements driver.Conn.
type DropDBConn struct {
	db *server.DropDB
}

// Prepare returns a prepared statement, but we'll simply store the SQL string.
// Actual planning happens in Stmt.Exec / Stmt.Query (auto-commit style).
func (c *DropDBConn) Prepare(query string) (driver.Stmt, error) {
	return &DropDBStmt{
		conn:  c,
		query: query,
	}, nil
}

// Close is called when database/sql is done with this connection.
func (c *DropDBConn) Close() error {
	// There's no real "closing" an embedded DB, but if you had
	// a long-running Tx or resources pinned, you could clean them up here.
	return nil
}

// Begin starts a transaction. If you want to support explicit
// transaction semantics (e.g. db.Begin(), tx.Commit(), tx.Rollback()),
// you can do so here. We'll do a simple pass-through model to a new Tx.
func (c *DropDBConn) Begin() (driver.Tx, error) {
	// Start a brand new DropDB transaction
	t := c.db.NewTx()
	return &DropDBTx{tx: t}, nil
}
