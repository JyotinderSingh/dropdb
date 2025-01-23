package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/types"
)

var _ plan.Plan = &SelectPlan{}

type SelectPlan struct {
	inputPlan plan.Plan
	predicate *query.Predicate
}

// NewSelectPlan creates a new select node in the query tree,
// having the specified subquery and predicate.
func NewSelectPlan(inputPlan plan.Plan, predicate *query.Predicate) *SelectPlan {
	return &SelectPlan{
		inputPlan: inputPlan,
		predicate: predicate,
	}
}

// Open creates a select scan for this query.
func (sp *SelectPlan) Open() (scan.Scan, error) {
	inputScan, err := sp.inputPlan.Open()
	if err != nil {
		return nil, err
	}
	return query.NewSelectScan(inputScan, sp.predicate)
}

// BlocksAccessed estimates the number of block accesses in the selection,
// which is the same as in the underlying query.
func (sp *SelectPlan) BlocksAccessed() int {
	return sp.inputPlan.BlocksAccessed()
}

// RecordsOutput estimates the number of records in the selection,
// which is determined by the reduction factor of the predicate.
func (sp *SelectPlan) RecordsOutput() int {
	return sp.inputPlan.RecordsOutput() / sp.predicate.ReductionFactor(sp.inputPlan)
}

// DistinctValues estimates the number of distinct values in the projection.
// This is a heuristic estimate based on the predicate. It's not always accurate.
// We can probably improve this estimate by considering the actual data.
func (sp *SelectPlan) DistinctValues(fieldName string) int {
	// 1) If there's an equality check for fieldName = constant, it's 1 distinct value.
	if sp.predicate.EquatesWithConstant(fieldName) != nil {
		return 1
	}

	// 2) If there's an equality check for fieldName = someOtherField
	fieldName2 := sp.predicate.EquatesWithField(fieldName)
	if fieldName2 != "" {
		return min(
			sp.inputPlan.DistinctValues(fieldName),
			sp.inputPlan.DistinctValues(fieldName2),
		)
	}

	// 3) Check for range comparisons (fieldName < c, > c, <= c, >= c, <> c, etc.)
	op, _ := sp.predicate.ComparesWithConstant(fieldName)
	switch op {
	case types.LT, types.LE, types.GT, types.GE:
		// A naive heuristic: cut the number of distinct values in half
		// because we're restricting to a range.
		return max(1, sp.inputPlan.DistinctValues(fieldName)/2)

	case types.NE:
		// “not equal” typically leaves most of the domain, but at least
		// it excludes 1 distinct value if we know which constant is being excluded.
		distinct := sp.inputPlan.DistinctValues(fieldName)
		if distinct > 1 {
			return distinct - 1
		}
		return 1 // if there's only 1 or 0 possible distinct values, clamp to 1

	default:
		// If there's no relevant range comparison or none is recognized,
		// fall back to the underlying plan’s estimate.
		return sp.inputPlan.DistinctValues(fieldName)
	}
}

// Schema returns the schema of the selection,
// which is the same as the schema of the underlying query.
func (sp *SelectPlan) Schema() *record.Schema {
	return sp.inputPlan.Schema()
}
