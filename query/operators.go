package query

// Operator is the type of Operator used in a term.
type Operator int

const (
	// EQ is the equal Operator.
	EQ Operator = iota
	// NE is the not equal Operator.
	NE
	// LT is the less than Operator.
	LT
	// LE is the less than or equal Operator.
	LE
	// GT is the greater than Operator.
	GT
	// GE is the greater than or equal Operator.
	GE
)

// String returns the string representation of the Operator.
func (op Operator) String() string {
	switch op {
	case EQ:
		return "="
	case NE:
		return "<>"
	case LT:
		return "<"
	case LE:
		return "<="
	case GT:
		return ">"
	case GE:
		return ">="
	default:
		return ""
	}
}