package dropdbdriver

import "errors"

// DropDBResult implements driver.Result for the Exec path.
type DropDBResult struct {
	rowsAffected int64
}

// LastInsertId is not implemented yet.
func (r *DropDBResult) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId not supported")
}

// RowsAffected returns how many rows were changed by the statement.
func (r *DropDBResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
