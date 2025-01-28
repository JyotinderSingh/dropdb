package driver

import (
	"database/sql/driver"
	"errors"
	"github.com/JyotinderSingh/dropdb/server"
	"github.com/JyotinderSingh/dropdb/tx"
)

// DropDBConn implements driver.Conn.
type DropDBConn struct {
	db *server.DropDB

	// activeTx is non-nil if we are in an explicit transaction
	activeTx *tx.Transaction
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

// Begin starts a transaction
func (c *DropDBConn) Begin() (driver.Tx, error) {
	if c.activeTx != nil {
		// either error or nested transactions if supported
		return nil, errors.New("already in a transaction")
	}
	newTx := c.db.NewTx()
	c.activeTx = newTx
	return &DropDBTx{
		conn: c,
		tx:   newTx,
	}, nil
}
