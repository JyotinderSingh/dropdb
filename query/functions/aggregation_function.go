package functions

import "github.com/JyotinderSingh/dropdb/scan"

type AggregationFunction interface {
	// ProcessFirst uses the current record of the
	// specified scan to be the first record in the group.
	ProcessFirst(s scan.Scan) error

	// ProcessNext uses the current record of the
	// specified scan to be the next record in the group.
	ProcessNext(s scan.Scan) error

	// FieldName returns the name of the new aggregation
	// field.
	FieldName() string

	// Value returns the computed aggregation value.
	Value() any
}
