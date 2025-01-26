package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/materialize"
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/tx"
	"math"
)

// MaterializePlan represents the Plan for the materialize operator.
type MaterializePlan struct {
	srcPlan plan.Plan
	tx      *tx.Transaction
}

// NewMaterializePlan creates a materialize plan for the specified query.
func NewMaterializePlan(tx *tx.Transaction, srcPlan plan.Plan) *MaterializePlan {
	return &MaterializePlan{
		srcPlan: srcPlan,
		tx:      tx,
	}
}

// Open loops through the underlying query, copying its output records into a temporary table.
// It then returns a table scan for that table.
func (mp *MaterializePlan) Open() (scan.Scan, error) {
	schema := mp.srcPlan.Schema()
	tempTable := materialize.NewTempTable(mp.tx, schema)
	srcScan, err := mp.srcPlan.Open()
	if err != nil {
		return nil, err
	}
	defer srcScan.Close()

	destinationScan, err := tempTable.Open()
	if err != nil {
		return nil, err
	}

	for {
		hasNext, err := srcScan.Next()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		if err := destinationScan.Insert(); err != nil {
			return nil, err
		}
		for _, fieldName := range schema.Fields() {
			val, err := srcScan.GetVal(fieldName)
			if err != nil {
				return nil, err
			}
			if err := destinationScan.SetVal(fieldName, val); err != nil {
				return nil, err
			}
		}
	}

	if err := destinationScan.BeforeFirst(); err != nil {
		return nil, err
	}
	return destinationScan, nil
}

// BlocksAccessed returns the estimated number of blocks in the materialized table.
func (mp *MaterializePlan) BlocksAccessed() int {
	// create a fake layout to calculate the record size
	layout := record.NewLayout(mp.srcPlan.Schema())
	recordLength := layout.SlotSize()
	recordsPerBlock := float64(mp.tx.BlockSize()) / float64(recordLength)
	return int(math.Ceil(float64(mp.srcPlan.RecordsOutput()) / recordsPerBlock))
}

// RecordsOutput returns the number of records in the materialized table.
func (mp *MaterializePlan) RecordsOutput() int {
	return mp.srcPlan.RecordsOutput()
}

// DistinctValues returns the number of distinct field values, which is the same as the underlying plan.
func (mp *MaterializePlan) DistinctValues(fieldName string) int {
	return mp.srcPlan.DistinctValues(fieldName)
}

// Schema returns the schema of the materialized table, which is the same as the underlying plan.
func (mp *MaterializePlan) Schema() *record.Schema {
	return mp.srcPlan.Schema()
}
