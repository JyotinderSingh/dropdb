package functions

import (
	"github.com/JyotinderSingh/dropdb/scan"
)

var _ AggregationFunction = &CountFunction{}

const countFunctionPrefix = "countOf"

type CountFunction struct {
	fieldName string
	count     int64
}

// NewCountFunction creates a new count aggregation function for the specified field.
// Some implementations ignore the fieldName if they want to count *all* rows.
func NewCountFunction(fieldName string) *CountFunction {
	return &CountFunction{
		fieldName: fieldName,
	}
}

// ProcessFirst initializes the count to 1.
func (f *CountFunction) ProcessFirst(s scan.Scan) error {
	f.count = 1
	return nil
}

// ProcessNext increments the count by 1.
func (f *CountFunction) ProcessNext(s scan.Scan) error {
	f.count++
	return nil
}

// FieldName returns a name like "countOf<field>".
func (f *CountFunction) FieldName() string {
	return countFunctionPrefix + f.fieldName
}

// Value returns the current count.
func (f *CountFunction) Value() any {
	return f.count
}
