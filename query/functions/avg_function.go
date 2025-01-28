package functions

import (
	"github.com/JyotinderSingh/dropdb/scan"
)

var _ AggregationFunction = &AvgFunction{}

const avgFunctionPrefix = "avgOf"

type AvgFunction struct {
	fieldName string
	sum       int
	count     int
}

// NewAvgFunction creates a new avg aggregation function for the specified field.
func NewAvgFunction(fieldName string) *AvgFunction {
	return &AvgFunction{
		fieldName: fieldName,
	}
}

// ProcessFirst sets the initial sum and count.
func (f *AvgFunction) ProcessFirst(s scan.Scan) error {
	val, err := s.GetVal(f.fieldName)
	if err != nil {
		return err
	}
	numVal, err := toInt(val)
	if err != nil {
		return err
	}
	f.sum = numVal
	f.count = 1
	return nil
}

// ProcessNext adds the field value to the sum and increments the count.
func (f *AvgFunction) ProcessNext(s scan.Scan) error {
	val, err := s.GetVal(f.fieldName)
	if err != nil {
		return err
	}
	numVal, err := toInt(val)
	if err != nil {
		return err
	}
	f.sum += numVal
	f.count++
	return nil
}

// FieldName returns the field's name, prepended by avgFunctionPrefix.
func (f *AvgFunction) FieldName() string {
	return avgFunctionPrefix + f.fieldName
}

// Value returns the current average as a float64 (or int, depending on your needs).
// TODO: Casts value to int for now since our database doesnt support floats yet..
func (f *AvgFunction) Value() any {
	if f.count == 0 {
		return 0 // or error if no rows
	}
	return int(f.sum / f.count)
}
