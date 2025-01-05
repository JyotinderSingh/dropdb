package query

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
)

type Expression struct {
	value     any
	fieldName string
}

// NewFieldExpression creates a new expression for a field name.
func NewFieldExpression(fieldName string) *Expression {
	return &Expression{value: nil, fieldName: fieldName}
}

// NewConstantExpression creates a new expression for a constant value.
func NewConstantExpression(value any) *Expression {
	return &Expression{value: value, fieldName: ""}
}

// Evaluate the expression with respect to the current record of the specified inputScan.
func (e *Expression) Evaluate(inputScan scan.Scan) (any, error) {
	if e.value != nil {
		return e.value, nil
	}
	return inputScan.GetVal(e.fieldName)
}

// IsFieldName returns true if the expression is a field reference.
func (e *Expression) IsFieldName() bool {
	return e.fieldName != ""
}

// IsConstant returns true if the expression is a constant expression,
// or nil if the expression does not denote a constant.
func (e *Expression) asConstant() any {
	return e.value
}

// IsFieldName returns the field name if the expression is a field reference,
// or an empty string if the expression does not denote a field.
func (e *Expression) asFieldName() string {
	return e.fieldName
}

// AppliesTo determines if all the fields mentioned in this expression are contained in the specified schema.
func (e *Expression) AppliesTo(schema *record.Schema) bool {
	return e.value != nil || schema.HasField(e.fieldName)
}

func (e *Expression) String() string {
	if e.value != nil {
		return fmt.Sprintf("%v", e.value)
	}
	return e.fieldName
}
