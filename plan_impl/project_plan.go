package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
)

var _ plan.Plan = &ProjectPlan{}

type ProjectPlan struct {
	inputPlan plan.Plan
	schema    *record.Schema
}

// NewProjectPlan creates a new project node in the query tree,
// having the specified subquery and field list.
func NewProjectPlan(inputPlan plan.Plan, fieldList []string) (*ProjectPlan, error) {
	pp := &ProjectPlan{inputPlan: inputPlan, schema: record.NewSchema()}

	for _, fieldName := range fieldList {
		pp.schema.Add(fieldName, inputPlan.Schema())
	}

	return pp, nil
}

// Open creates a project scan for this query.
func (pp *ProjectPlan) Open() (scan.Scan, error) {
	inputScan, err := pp.inputPlan.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProjectScan(inputScan, pp.schema.Fields())
}

// BlocksAccessed estimates the number of block accesses in the projection,
// which is the same as in the underlying query.
func (pp *ProjectPlan) BlocksAccessed() int {
	return pp.inputPlan.BlocksAccessed()
}

// RecordsOutput estimates the number of records in the projection,
// which is the same as in the underlying query.
func (pp *ProjectPlan) RecordsOutput() int {
	return pp.inputPlan.RecordsOutput()
}

// DistinctValues estimates the number of distinct values in the projection,
// which is the same as in the underlying query.
func (pp *ProjectPlan) DistinctValues(fieldName string) int {
	return pp.inputPlan.DistinctValues(fieldName)
}

// Schema returns the schema of the projection,
// which is taken from the field list.
func (pp *ProjectPlan) Schema() *record.Schema {
	return pp.schema
}
