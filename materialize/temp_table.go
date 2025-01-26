package materialize

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/table"
	"github.com/JyotinderSingh/dropdb/tx"
	"sync"
)

const tempTablePrefix = "temp"

// TempTable represents a temporary table not registered in the catalog.
type TempTable struct {
	tx      *tx.Transaction
	tblName string
	layout  *record.Layout
}

var (
	nextTableNum   = 0
	nextTableNumMu sync.Mutex
)

// NewTempTable creates a new temporary table with the specified schema and transaction.
func NewTempTable(tx *tx.Transaction, schema *record.Schema) *TempTable {
	return &TempTable{
		tx:      tx,
		tblName: nextTableName(),
		layout:  record.NewLayout(schema),
	}
}

// Open opens a table scan for the temporary table.
func (tt *TempTable) Open() (scan.UpdateScan, error) {
	return table.NewTableScan(tt.tx, tt.tblName, tt.layout)
}

// TableName returns the name of the temporary table.
func (tt *TempTable) TableName() string {
	return tt.tblName
}

// GetLayout returns the table's metadata (layout).
func (tt *TempTable) GetLayout() *record.Layout {
	return tt.layout
}

// nextTableName generates a unique name for the next temporary table.
func nextTableName() string {
	nextTableNumMu.Lock()
	defer nextTableNumMu.Unlock()
	nextTableNum++
	return fmt.Sprintf("%s%d", tempTablePrefix, nextTableNum)
}
