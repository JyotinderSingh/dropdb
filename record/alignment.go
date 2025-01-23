package record

import (
	"github.com/JyotinderSingh/dropdb/types"
)

// Data type alignments in bytes (platform-independent where possible)
const (
	LongAlignment    = 8
	ShortAlignment   = 2
	BooleanAlignment = 1
	DateAlignment    = 8
	VarcharAlignment = 1 // No alignment for strings, packed tightly
)

// alignmentRequirement returns the alignment size for a given field type.
func alignmentRequirement(fieldType types.SchemaType) int {
	switch fieldType {
	case types.Integer:
		return types.IntSize
	case types.Long:
		return LongAlignment
	case types.Short:
		return ShortAlignment
	case types.Boolean:
		return BooleanAlignment
	case types.Date:
		return DateAlignment
	case types.Varchar:
		return VarcharAlignment
	default:
		return 1 // Default to no alignment for unknown types
	}
}

// Helper function to find the maximum alignment from the map
func maxAlignment(fieldAlignments map[string]int) int {
	maxAlign := 1
	for _, align := range fieldAlignments {
		if align > maxAlign {
			maxAlign = align
		}
	}
	return maxAlign
}
