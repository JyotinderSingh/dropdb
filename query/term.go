package query

import (
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/types"
)

type Term struct {
	lhs *Expression
	rhs *Expression
	op  types.Operator
}

// NewTerm creates a new term.
func NewTerm(lhs, rhs *Expression, op types.Operator) *Term {
	return &Term{lhs: lhs, rhs: rhs, op: op}
}

func (t *Term) IsSatisfied(inputScan scan.Scan) bool {
	var lhsVal, rhsVal any
	var err error
	if lhsVal, err = t.lhs.Evaluate(inputScan); err != nil {
		return false
	}

	if rhsVal, err = t.rhs.Evaluate(inputScan); err != nil {
		return false
	}

	switch t.op {
	case types.EQ:
		return lhsVal == rhsVal
	case types.NE:
		return lhsVal != rhsVal
	case types.LT, types.LE, types.GT, types.GE:
		return types.CompareSupportedTypes(lhsVal, rhsVal, t.op)
	default:
		return false
	}
}

// ReductionFactor calculates the extent to which selecting on the term reduces
// the number of records output by a query.
// For example if the reduction factor is 2, then the term cuts the size of the
// output in half. If the reduction factor is 1, then the term has no effect.
func (t *Term) ReductionFactor(queryPlan plan.Plan) int {
	var lhsName, rhsName string

	// If both sides are field names, calculate the max distinct values.
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		lhsName = t.lhs.asFieldName()
		rhsName = t.rhs.asFieldName()
		return max(queryPlan.DistinctValues(lhsName), queryPlan.DistinctValues(rhsName))
	}

	// If LHS is a field name, use its distinct values.
	if t.lhs.IsFieldName() {
		lhsName = t.lhs.asFieldName()
		return reductionForConstantComparison(queryPlan.DistinctValues(lhsName), t.op)
	}

	// If RHS is a field name, use its distinct values.
	if t.rhs.IsFieldName() {
		rhsName = t.rhs.asFieldName()
		return reductionForConstantComparison(queryPlan.DistinctValues(rhsName), t.op)
	}

	// Handle constant comparisons
	lhsConst := t.lhs.asConstant()
	rhsConst := t.rhs.asConstant()

	// If constants are equal for EQ, perfect selectivity; otherwise, default.
	if lhsConst == rhsConst && t.op == types.EQ {
		return 1
	}
	if lhsConst != rhsConst && t.op == types.NE {
		return 1
	}

	// Default case for constant-to-constant comparisons.
	return int(^uint(0) >> 1) // High value for poor selectivity
}

// Helper to calculate reduction factor for constant comparisons using distinct values.
func reductionForConstantComparison(distinctValues int, op types.Operator) int {
	switch op {
	case types.EQ:
		return max(1, distinctValues)
	case types.NE:
		// Assumes non-equality doesn't significantly reduce distinct values.
		if distinctValues <= 1 {
			return 1
		} else {
			// approximate: the portion we keep is (distinctValues-1)/distinctValues,
			// so the factor = 1 / that portion = distinctValues/(distinctValues-1)
			return distinctValues / (distinctValues - 1)
		}
	case types.LT, types.LE, types.GT, types.GE:
		// Assume uniform distribution; halve the distinct values for range operators.
		return 2
	default:
		return 1 // Default for unsupported operators, assume no reduction.
	}
}

// EquatesWithConstant determines if this term is of the form "F=c"
// where F is the specified field and c is some constant.
// If so, the method returns that constant.
// If not, the method returns nil.
func (t *Term) EquatesWithConstant(fieldName string) any {
	if t.op != types.EQ { // Explicit check for equality
		return nil
	}
	if t.lhs.IsFieldName() && t.lhs.asFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.asConstant()
	} else if t.rhs.IsFieldName() && t.rhs.asFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.lhs.asConstant()
	}
	return nil
}

// ComparesWithConstant determines if this term is of the form "F1 < 100"
func (t *Term) ComparesWithConstant(fieldName string) (types.Operator, any) {
	// Check if this Term involves the given fieldName on one side
	// and a *constant* on the other side, e.g. "F1 < 100".
	// If so, return (operator, constant).
	// If not, return (NONE, nil).

	// LHS is the field, RHS is a constant
	if t.lhs.IsFieldName() && t.lhs.asFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.op, t.rhs.asConstant()
	}
	// RHS is the field, LHS is a constant
	if t.rhs.IsFieldName() && t.rhs.asFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.op, t.lhs.asConstant()
	}
	return types.NONE, nil
}

// EquatesWithField determines if this term is of the form "F1=F2"
// where F1 is the specified field and F2 is another field.
// If so, the method returns the name of the other field.
// If not, the method returns an empty string.
func (t *Term) EquatesWithField(fieldName string) string {
	if t.op != types.EQ { // Explicit check for equality
		return ""
	}
	if t.lhs.IsFieldName() && t.lhs.asFieldName() == fieldName && t.rhs.IsFieldName() {
		return t.rhs.asFieldName()
	} else if t.rhs.IsFieldName() && t.rhs.asFieldName() == fieldName && t.lhs.IsFieldName() {
		return t.lhs.asFieldName()
	}
	return ""
}

// AppliesTo returns true if both of the term's expressions
// apply to the specified schema.
func (t *Term) AppliesTo(schema *record.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

func (t *Term) String() string {
	return t.lhs.String() + " " + t.op.String() + " " + t.rhs.String()
}
