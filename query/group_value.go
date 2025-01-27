package query

import (
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/types"
)

type GroupValue struct {
	values map[string]any
}

// NewGroupValue creates a new group value, given the specified scan
// and list of fields.
// The values in the current record of each field are stored.
func NewGroupValue(s scan.Scan, fields []string) (*GroupValue, error) {
	values := make(map[string]any)
	for _, field := range fields {
		value, err := s.GetVal(field)
		if err != nil {
			return nil, err
		}
		values[field] = value
	}
	return &GroupValue{values: values}, nil
}

// GetVal returns the value of the specified field in the group.
func (g *GroupValue) GetVal(field string) any {
	if val, ok := g.values[field]; !ok {
		return nil
	} else {
		return val
	}
}

// Equals compares the specified group value with this one. Two group
// values are equal if they have the same values for their grouping fields.
func (g *GroupValue) Equals(other any) bool {
	otherGroup, ok := other.(*GroupValue)
	if !ok {
		return false
	}

	for field, value := range g.values {
		value2 := otherGroup.GetVal(field)
		if types.CompareSupportedTypes(value, value2, types.NE) {
			return false
		}
	}

	return true
}

// Hash returns a hash value for the group value.
// The hash of a GroupValue is the sum of hashes of its field values.
func (g *GroupValue) Hash() int {
	hash := 0
	for _, value := range g.values {
		hash += types.Hash(value)
	}
	return hash
}
