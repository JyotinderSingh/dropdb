package query

import (
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
)

type Predicate struct {
	terms []*Term
}

// NewPredicate creates an empty predicate, corresponding to TRUE.
func NewPredicate() *Predicate {
	return &Predicate{terms: []*Term{}}
}

// NewPredicateFromTerm creates a new predicate from the specified term.
func NewPredicateFromTerm(term *Term) *Predicate {
	return &Predicate{terms: []*Term{term}}
}

// CojoinWith modifies the predicate to be the conjunction of itself and the specified predicate.
func (p *Predicate) CojoinWith(other *Predicate) {
	p.terms = append(p.terms, other.terms...)
}

// IsSatisfied returns true if the predicate evaluates to true with respect to the specified inputScan.
func (p *Predicate) IsSatisfied(inputScan scan.Scan) bool {
	for _, term := range p.terms {
		if !term.IsSatisfied(inputScan) {
			return false
		}
	}
	return true
}

// ReductionFactor calculates the extent to which selecting on the
// predicate reduces the number of records output by a query.
// For example, if the reduction factor is 2, then the predicate
// cuts the size of the output in half.
// If the reduction factor is 1, then the predicate has no effect.
func (p *Predicate) ReductionFactor(queryPlan plan.Plan) int {
	factor := 1
	for _, term := range p.terms {
		factor *= term.ReductionFactor(queryPlan)
	}
	return factor
}

// SelectSubPredicate returns the sub-predicate that applies to the specified schema.
func (p *Predicate) SelectSubPredicate(schema *record.Schema) *Predicate {
	result := NewPredicate()
	for _, term := range p.terms {
		if term.AppliesTo(schema) {
			result.terms = append(result.terms, term)
		}
	}

	if len(result.terms) == 0 {
		return nil
	}

	return result
}

// JoinSubPredicate returns the sub-predicate consisting of terms
// that apply to the union of the two specified schemas,
// but not to either schema separately.
func (p *Predicate) JoinSubPredicate(schema1, schema2 *record.Schema) *Predicate {
	result := NewPredicate()
	unionSchema := record.NewSchema()

	unionSchema.AddAll(schema1)
	unionSchema.AddAll(schema2)

	for _, term := range p.terms {
		if !term.AppliesTo(schema1) && !term.AppliesTo(schema2) && term.AppliesTo(unionSchema) {
			result.terms = append(result.terms, term)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

// EquatesWithConstant determines if there is a term of the form "F=c"
// where F is the specified field and c is some constant.
// If so, the constant is returned; otherwise, nil is returned.
func (p *Predicate) EquatesWithConstant(fieldName string) any {
	for _, term := range p.terms {
		if c := term.EquatesWithConstant(fieldName); c != nil {
			return c
		}
	}
	return nil
}

// EquatesWithField determines if there is a term of the form "F1=F2"
// where F1 is the specified field and F2 is another field.
// If so, the name of the other field is returned; otherwise, an empty string is returned.
func (p *Predicate) EquatesWithField(fieldName string) string {
	for _, term := range p.terms {
		if f := term.EquatesWithField(fieldName); f != "" {
			return f
		}
	}
	return ""
}

// String returns a string representation of the predicate.
func (p *Predicate) String() string {
	if len(p.terms) == 0 {
		return ""
	}

	result := p.terms[0].String()
	for _, term := range p.terms[1:] {
		result += " and " + term.String()
	}

	return result
}
