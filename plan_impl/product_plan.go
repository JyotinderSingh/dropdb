package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
)

var _ plan.Plan = &ProductPlan{}

type ProductPlan struct {
	plan1  plan.Plan
	plan2  plan.Plan
	schema *record.Schema
}

// NewProductPlan creates a new product node in the query tree,
// having the specified subqueries.
func NewProductPlan(plan1 plan.Plan, plan2 plan.Plan) (*ProductPlan, error) {
	pp := &ProductPlan{plan1: plan1, plan2: plan2, schema: record.NewSchema()}
	pp.schema.AddAll(plan1.Schema())
	pp.schema.AddAll(plan2.Schema())
	return pp, nil
}

// Open creates a product scan for this query.
func (pp *ProductPlan) Open() (scan.Scan, error) {
	s1, err := pp.plan1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := pp.plan2.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProductScan(s1, s2), nil
}

// BlocksAccessed estimates the number of block accesses in the product,
// The formula is: blocks(plan1) + records(plan1) * blocks(plan2).
func (pp *ProductPlan) BlocksAccessed() int {
	return pp.plan1.BlocksAccessed() + pp.plan1.RecordsOutput()*pp.plan2.BlocksAccessed()
}

// RecordsOutput estimates the number of records in the product.
// The formula is: records(plan1) * records(plan2).
func (pp *ProductPlan) RecordsOutput() int {
	return pp.plan1.RecordsOutput() * pp.plan2.RecordsOutput()
}

// DistinctValues estimates the number of distinct field values in the product.
// Since the product does not increase or decrease field valuese,
// the estimate is the same as in the appropriate subplan.
func (pp *ProductPlan) DistinctValues(fieldName string) int {
	if pp.plan1.Schema().HasField(fieldName) {
		return pp.plan1.DistinctValues(fieldName)
	}
	return pp.plan2.DistinctValues(fieldName)
}

// Schema returns the schema of the product,
// which is the concatenation subplans' schemas.
func (pp *ProductPlan) Schema() *record.Schema {
	return pp.schema
}
