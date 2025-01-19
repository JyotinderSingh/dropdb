package dropdbdriver

import "github.com/JyotinderSingh/dropdb/tx"

// DropDBTx implements driver.Tx so that database/sql can manage
// a transaction with Commit() and Rollback().
type DropDBTx struct {
	tx *tx.Transaction
}

// Commit commits the current DropDB transaction
func (t *DropDBTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the current DropDB transaction
func (t *DropDBTx) Rollback() error {
	return t.tx.Rollback()
}
