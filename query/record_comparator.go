package query

import (
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/types"
)

// RecordComparator is a comparator for scans based on a list of field names.
type RecordComparator struct {
	fields []string
}

// NewRecordComparator creates a new comparator using the specified fields.
func NewRecordComparator(fields []string) *RecordComparator {
	return &RecordComparator{fields: fields}
}

// Compare compares the current records of two scans based on the specified fields. Expects supported types.
func (rc *RecordComparator) Compare(s1, s2 scan.Scan) int {
	for _, fieldName := range rc.fields {
		// Get values for the current field
		val1, err1 := s1.GetVal(fieldName)
		val2, err2 := s2.GetVal(fieldName)

		if err1 != nil || err2 != nil {
			panic("Error retrieving field values for comparison")
		}

		// Compare using CompareSupportedTypes with equality and ordering operators
		if types.CompareSupportedTypes(val1, val2, types.LT) {
			return -1 // val1 < val2
		} else if types.CompareSupportedTypes(val1, val2, types.GT) {
			return 1 // val1 > val2
		}
		// If neither LT nor GT, the values must be equal for this field; continue to next field.
	}
	return 0 // All fields are equal
}
