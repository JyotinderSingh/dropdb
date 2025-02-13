package functions

import (
	"fmt"

	"github.com/JyotinderSingh/dropdb/scan"
)

var _ AggregationFunction = &SumFunction{}

const sumFunctionPrefix = "sumOf"

type SumFunction struct {
	fieldName string
	sum       int // Using int to make it simpler to handle types across the db. This might cause issues with 64-bit integers on 32-bit architectures.
}

// NewSumFunction creates a new sum aggregation function for the specified field.
func NewSumFunction(fieldName string) *SumFunction {
	return &SumFunction{
		fieldName: fieldName,
	}
}

// ProcessFirst sets the initial sum to the field value in the current record.
func (f *SumFunction) ProcessFirst(s scan.Scan) error {
	val, err := s.GetVal(f.fieldName)
	if err != nil {
		return err
	}
	intVal, err := toInt(val)
	if err != nil {
		return err
	}
	f.sum = intVal
	return nil
}

// ProcessNext adds the field value in the current record to the running sum.
func (f *SumFunction) ProcessNext(s scan.Scan) error {
	val, err := s.GetVal(f.fieldName)
	if err != nil {
		return err
	}
	intVal, err := toInt(val)
	if err != nil {
		return err
	}
	f.sum += intVal
	return nil
}

// FieldName returns the field's name, prepended by sumFunctionPrefix.
func (f *SumFunction) FieldName() string {
	return sumFunctionPrefix + f.fieldName
}

// Value returns the current sum.
func (f *SumFunction) Value() any {
	return f.sum
}

// Helper to handle int, int16, int64, or possibly other numeric types.
// Using int to make it simpler to handle types across the db.
// This might cause issues with 64-bit integers on 32-bit architectures.
func toInt(v any) (int, error) {
	switch num := v.(type) {
	case int:
		return num, nil
	case int16:
		return int(num), nil
	case int64:
		return int(num), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64 for sum", v)
	}
}
