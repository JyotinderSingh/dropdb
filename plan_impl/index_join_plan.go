package plan_impl

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/table"
)

var _ plan.Plan = &IndexJoinPlan{}

// IndexJoinPlan is a plan that corresponds to an index join operation.
type IndexJoinPlan struct {
	plan1     plan.Plan
	plan2     plan.Plan
	indexInfo metadata.IndexInfo
	joinField string
	schema    *record.Schema
}

// NewIndexJoinPlan creates a new IndexJoinPlan with the given plans and index info
func NewIndexJoinPlan(plan1, plan2 plan.Plan, indexInfo metadata.IndexInfo, joinField string) *IndexJoinPlan {
	ijp := &IndexJoinPlan{
		plan1:     plan1,
		plan2:     plan2,
		indexInfo: indexInfo,
		joinField: joinField,
		schema:    record.NewSchema(),
	}

	ijp.schema.AddAll(plan1.Schema())
	ijp.schema.AddAll(plan2.Schema())

	return ijp
}

// Open opens an index join scan for this query.
func (ijp *IndexJoinPlan) Open() (scan.Scan, error) {
	s1, err := ijp.plan1.Open()
	if err != nil {
		return nil, err
	}

	s2, err := ijp.plan2.Open()
	if err != nil {
		return nil, err
	}
	tableScan, ok := s2.(*table.Scan)
	if !ok {
		return nil, fmt.Errorf("first plan is not a table scan")
	}

	idx := ijp.indexInfo.Open()

	return query.NewIndexJoinScan(s1, tableScan, ijp.joinField, idx)
}

// BlocksAccessed estimates the number of block access to compute the join.
// The formula is
// blocks(indexjoin(p1, p2, idx)) = blocks(p1) + Rows(p1)*blocks(idx) + rows(indexjoin(p1, p2, idx))
func (ijp *IndexJoinPlan) BlocksAccessed() int {
	return ijp.plan1.BlocksAccessed() + (ijp.plan1.RecordsOutput() * ijp.indexInfo.BlocksAccessed()) + ijp.RecordsOutput()
}

// RecordsOutput estimates the number of output records after performing the join.
// The formula is
// rows(indexjoin(p1, p2, idx)) = rows(p1) * rows(idx)
func (ijp *IndexJoinPlan) RecordsOutput() int {
	return ijp.plan1.RecordsOutput() * ijp.indexInfo.RecordsOutput()
}

// DistinctValues estimates the number of distinct values for the specified field.
func (ijp *IndexJoinPlan) DistinctValues(fieldName string) int {
	if ijp.plan1.Schema().HasField(fieldName) {
		return ijp.plan1.DistinctValues(fieldName)
	}
	return ijp.plan2.DistinctValues(fieldName)
}

// Schema returns the schema for the index join plan.
func (ijp *IndexJoinPlan) Schema() *record.Schema {
	return ijp.schema
}
