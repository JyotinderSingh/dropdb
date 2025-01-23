package types

import "fmt"

// Operator is the type of Operator used in a term.
type Operator int

const (
	NONE Operator = -1
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

// OperatorFromString returns the Operator from the given string.
func OperatorFromString(op string) (Operator, error) {
	switch op {
	case "=":
		return EQ, nil
	case "<>", "!=":
		return NE, nil
	case "<":
		return LT, nil
	case "<=":
		return LE, nil
	case ">":
		return GT, nil
	case ">=":
		return GE, nil
	default:
		return -1, fmt.Errorf("invalid operator: %s", op)
	}
}
