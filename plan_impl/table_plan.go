package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/table"
	"github.com/JyotinderSingh/dropdb/tx"
)

var _ plan.Plan = &TablePlan{}

type TablePlan struct {
	tableName   string
	transaction *tx.Transaction
	layout      *record.Layout
	statInfo    *metadata.StatInfo
}

// NewTablePlan creates a leaf node in the query tree
// corresponding to the specified table.
func NewTablePlan(transaction *tx.Transaction, tableName string, metadataManager *metadata.Manager) (*TablePlan, error) {
	tp := &TablePlan{
		tableName:   tableName,
		transaction: transaction,
	}

	var err error
	if tp.layout, err = metadataManager.GetLayout(tableName, transaction); err != nil {
		return nil, err
	}
	if tp.statInfo, err = metadataManager.GetStatInfo(tableName, tp.layout, transaction); err != nil {
		return nil, err
	}
	return tp, nil
}

// Open creates a table scan for this query
func (tp *TablePlan) Open() (scan.Scan, error) {
	return table.NewTableScan(tp.transaction, tp.tableName, tp.layout)
}

// BlocksAccessed estimates the number of block accesses for the table,
// which is obtainable from the statistics manager.
func (tp *TablePlan) BlocksAccessed() int {
	return tp.statInfo.BlocksAccessed()
}

// RecordsOutput estimates the number of records in the table,
// which is obtainable from the statistics manager.
func (tp *TablePlan) RecordsOutput() int {
	return tp.statInfo.RecordsOutput()
}

// DistinctValues estimates the number of distinct values for the specified field
// in the table, which is obtainable from the stats manager.
func (tp *TablePlan) DistinctValues(fieldName string) int {
	return tp.statInfo.DistinctValues(fieldName)
}

// Schema determines the schema of the table,
// which is obtainable from the catalog manager
func (tp *TablePlan) Schema() *record.Schema {
	return tp.layout.Schema()
}
