package types

import (
	"fmt"
	"time"
)

// CompareSupportedTypes handles comparison for supported types.
func CompareSupportedTypes(lhs, rhs any, op Operator) bool {
	// Handle nil values explicitly
	if lhs == nil || rhs == nil {
		return false // Null comparisons always return false in SQL semantics
	}

	// Type-specific comparisons
	switch lhs := lhs.(type) {
	case int:
		if rhs, ok := rhs.(int); ok {
			return compareInts(lhs, rhs, op)
		}
	case int64:
		if rhs, ok := rhs.(int64); ok {
			return compareInt64s(lhs, rhs, op)
		}
	case int16:
		if rhs, ok := rhs.(int16); ok {
			return compareInt16s(lhs, rhs, op)
		}
	case string:
		if rhs, ok := rhs.(string); ok {
			return compareStrings(lhs, rhs, op)
		}
	case bool:
		if rhs, ok := rhs.(bool); ok {
			return compareBools(lhs, rhs, op)
		}
	case time.Time:
		if rhs, ok := rhs.(time.Time); ok {
			return compareTimes(lhs, rhs, op)
		}
	default:
		// Log unsupported type for debugging
		fmt.Printf("Unsupported type for comparison: lhs=%T, rhs=%T\n", lhs, rhs)
	}

	// Return false for unsupported or mismatched types
	return false
}

// compareInts compares two integers.
func compareInts(lhs, rhs int, op Operator) bool {
	switch op {
	case NE:
		return lhs != rhs
	case EQ:
		return lhs == rhs
	case LT:
		return lhs < rhs
	case LE:
		return lhs <= rhs
	case GT:
		return lhs > rhs
	case GE:
		return lhs >= rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}

// compareInt64s compares two int64 values.
func compareInt64s(lhs, rhs int64, op Operator) bool {
	switch op {
	case NE:
		return lhs != rhs
	case EQ:
		return lhs == rhs
	case LT:
		return lhs < rhs
	case LE:
		return lhs <= rhs
	case GT:
		return lhs > rhs
	case GE:
		return lhs >= rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}

// compareInt16s compares two int16 values.
func compareInt16s(lhs, rhs int16, op Operator) bool {
	switch op {
	case NE:
		return lhs != rhs
	case EQ:
		return lhs == rhs
	case LT:
		return lhs < rhs
	case LE:
		return lhs <= rhs
	case GT:
		return lhs > rhs
	case GE:
		return lhs >= rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}

// compareStrings compares two strings.
func compareStrings(lhs, rhs string, op Operator) bool {
	switch op {
	case NE:
		return lhs != rhs
	case EQ:
		return lhs == rhs
	case LT:
		return lhs < rhs
	case LE:
		return lhs <= rhs
	case GT:
		return lhs > rhs
	case GE:
		return lhs >= rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}

// compareBools compares two booleans (only equality comparisons make sense).
func compareBools(lhs, rhs bool, op Operator) bool {
	switch op {
	case EQ:
		return lhs == rhs
	case NE:
		return lhs != rhs
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false // Invalid for comparison operators like <, >
	}
}

// compareTimes compares two time.Time values.
func compareTimes(lhs, rhs time.Time, op Operator) bool {
	switch op {
	case NE:
		return !lhs.Equal(rhs)
	case EQ:
		return lhs.Equal(rhs)
	case LT:
		return lhs.Before(rhs)
	case LE:
		return lhs.Before(rhs) || lhs.Equal(rhs)
	case GT:
		return lhs.After(rhs)
	case GE:
		return lhs.After(rhs) || lhs.Equal(rhs)
	default:
		fmt.Printf("unsupported operator: %v\n", op)
		return false
	}
}
