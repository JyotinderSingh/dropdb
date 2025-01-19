package driver

import (
	"database/sql/driver"
	"fmt"
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/tx"
	"io"
)

type DropDBRows struct {
	stmt *DropDBStmt
	tx   *tx.Transaction

	scan scan.Scan
	plan plan.Plan
	done bool

	// We'll extract column names once.
	columns []string
}

// Columns returns the column names from the schema.
func (r *DropDBRows) Columns() []string {
	if r.columns == nil {
		sch := r.plan.Schema()
		fields := sch.Fields()
		r.columns = make([]string, len(fields))
		copy(r.columns, fields)
	}
	return r.columns
}

// Close is called by database/sql when the result set is done.
// We need to release the underlying scan and commit the transaction (auto-commit).
func (r *DropDBRows) Close() error {
	if r.done {
		return nil
	}
	r.done = true
	r.scan.Close()
	// We can commit the transaction to auto-commit.
	return r.tx.Commit()
}

// Next is called to advance the cursor and populate one row of data into 'dest'.
// 'Dest' must match the number and types of the columns.
func (r *DropDBRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	// Attempt to move to the next record
	hasNext, err := r.scan.Next()
	if err != nil {
		// On error, rollback so no partial commit
		_ = r.tx.Rollback()
		r.done = true
		return err
	}
	if !hasNext {
		// no more rows
		r.done = true
		// auto-commit
		if commitErr := r.tx.Commit(); commitErr != nil {
			return commitErr
		}
		return io.EOF
	}

	// We have another row. Extract each column from the scan.
	cols := r.Columns()
	for i, col := range cols {
		columnType := r.plan.Schema().Type(col)

		// Convert from scan's type to driver.Value
		var v interface{}
		switch columnType {
		case record.Integer:
			v, err = r.scan.GetInt(col)
			if err != nil {
				return err
			}
		case record.Varchar:
			v, err = r.scan.GetString(col)
			if err != nil {
				return err
			}
		case record.Boolean:
			v, err = r.scan.GetBool(col)
			if err != nil {
				return err
			}
		case record.Long:
			v, err = r.scan.GetLong(col)
			if err != nil {
				return err
			}
		case record.Short:
			v, err = r.scan.GetShort(col)
			if err != nil {
				return err
			}
		case record.Date:
			v, err = r.scan.GetDate(col)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported field type: %v", columnType)
		}
		dest[i] = v
	}
	return nil
}
