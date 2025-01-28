package driver

import "github.com/JyotinderSingh/dropdb/tx"

// DropDBTx implements driver.Tx so that database/sql can manage
// a transaction with Commit() and Rollback().
// It just holds a reference to the connection so we can clear activeTx on commit/rollback
type DropDBTx struct {
	conn *DropDBConn
	tx   *tx.Transaction
}

func (t *DropDBTx) Commit() error {
	err := t.tx.Commit()
	t.conn.activeTx = nil
	return err
}

func (t *DropDBTx) Rollback() error {
	err := t.tx.Rollback()
	t.conn.activeTx = nil
	return err
}
