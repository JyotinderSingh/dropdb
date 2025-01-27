package query

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/query/functions"
	"github.com/JyotinderSingh/dropdb/scan"
	"time"
)

var _ scan.Scan = &GroupByScan{}

// GroupByScan is the scan class for the GROUP BY operation.
type GroupByScan struct {
	inputScan            scan.Scan
	groupFields          []string
	aggregationFunctions []functions.AggregationFunction
	groupValue           *GroupValue
	moreGroups           bool
}

// NewGroupByScan creates a groupby scan, given a grouped table scan.
// A grouped table scan is a scan that returns records in the order
// of the grouping fields so that all rows belonging to the same group are contiguous.
func NewGroupByScan(inputScan scan.Scan, groupFields []string, aggregationFunctions []functions.AggregationFunction) (*GroupByScan, error) {
	s := &GroupByScan{
		inputScan:            inputScan,
		groupFields:          groupFields,
		aggregationFunctions: aggregationFunctions,
	}

	if err := s.BeforeFirst(); err != nil {
		return nil, err
	}

	return s, nil
}

// BeforeFirst positions the scan before the first group.
// Internally, the underlying scan is always
// positioned at the first record of a group, which
// means that this method moves to the first underlying record.
func (s *GroupByScan) BeforeFirst() error {
	var err error
	if err = s.inputScan.BeforeFirst(); err != nil {
		return err
	}

	s.moreGroups, err = s.inputScan.Next()
	return err
}

// Next moves to the next group.
// The key of the group is determined by the group values at the current record.
// The method repeatedly reads the underlying records until it encounters a
// record having a different key.
// The aggregation functions are called for each record in the group.
func (s *GroupByScan) Next() (bool, error) {
	if !s.moreGroups {
		return false, nil
	}

	// 1) Initialize each aggregator with the current record
	for _, function := range s.aggregationFunctions {
		if err := function.ProcessFirst(s.inputScan); err != nil {
			return false, err
		}
	}

	var err error

	// 2) Capture the grouping key from the current record
	s.groupValue, err = NewGroupValue(s.inputScan, s.groupFields)
	if err != nil {
		return false, err
	}

	// 3) Keep reading subsequent records as long as they belong to the same group key
	for {
		s.moreGroups, err = s.inputScan.Next()
		if err != nil {
			return false, err
		}
		if !s.moreGroups {
			// No more records in the underlying scan => we are done with this group
			return true, nil
		}

		// Check if the new record's group key matches the current group's key
		nextGroupValue, err := NewGroupValue(s.inputScan, s.groupFields)
		if err != nil {
			return false, err
		}

		if !s.groupValue.Equals(nextGroupValue) {
			// We found a different group -> we'll stop here,
			// so the next call to GroupByScan.Next() handles that new group
			return true, nil
		}

		// If it's the *same* group, update the aggregator(s) with this new row
		for _, function := range s.aggregationFunctions {
			if err := function.ProcessNext(s.inputScan); err != nil {
				return false, err
			}
		}
	}
}

// Close closes the scan by closing the underlying scan.
func (s *GroupByScan) Close() {
	s.inputScan.Close()
}

// GetVal gets the value of the specified field.
// If the field is a group field, then its value
// can be obtained from the saved group value.
// Otherwise, the value is obtained from the appropriate
// aggregation function.
func (s *GroupByScan) GetVal(field string) (any, error) {
	for _, groupField := range s.groupFields {
		if groupField == field {
			return s.groupValue.GetVal(field), nil
		}
	}

	for _, function := range s.aggregationFunctions {
		if function.FieldName() == field {
			return function.Value(), nil
		}
	}

	return nil, fmt.Errorf("field %s not found", field)
}

// GetInt gets the integer value of the specified field.
// If the field is a group field, then its value can be
// obtained from the saved group value. Otherwise, the
// value is obtained from the appropriate aggregation function.
func (s *GroupByScan) GetInt(field string) (int, error) {
	value, err := s.GetVal(field)
	if err != nil {
		return 0, err
	}

	castedValue, ok := value.(int)
	if !ok {
		return 0, fmt.Errorf("field %s is not an int", field)
	}

	return castedValue, nil
}

// GetString gets the string value of the specified field.
// If the field is a group field, then its value can be
// obtained from the saved group value. Otherwise, the
// value is obtained from the appropriate aggregation function.
func (s *GroupByScan) GetString(field string) (string, error) {
	value, err := s.GetVal(field)
	if err != nil {
		return "", err
	}

	castedValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("field %s is not a string", field)
	}

	return castedValue, nil
}

// GetShort gets the short value of the specified field.
// If the field is a group field, then its value can be
// obtained from the saved group value. Otherwise, the
// value is obtained from the appropriate aggregation function.
func (s *GroupByScan) GetShort(field string) (int16, error) {
	value, err := s.GetVal(field)
	if err != nil {
		return 0, err
	}

	castedValue, ok := value.(int16)
	if !ok {
		return 0, fmt.Errorf("field %s is not a short", field)
	}

	return castedValue, nil
}

// GetLong gets the long value of the specified field.
// If the field is a group field, then its value can be
// obtained from the saved group value. Otherwise, the
// value is obtained from the appropriate aggregation function.
func (s *GroupByScan) GetLong(field string) (int64, error) {
	value, err := s.GetVal(field)
	if err != nil {
		return 0, err
	}

	castedValue, ok := value.(int64)
	if !ok {
		return 0, fmt.Errorf("field %s is not a long", field)
	}

	return castedValue, nil
}

// GetBool gets the boolean value of the specified field.
// If the field is a group field, then its value can be
// obtained from the saved group value. Otherwise, the
// value is obtained from the appropriate aggregation function.
func (s *GroupByScan) GetBool(field string) (bool, error) {
	value, err := s.GetVal(field)
	if err != nil {
		return false, err
	}

	castedValue, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("field %s is not a bool", field)
	}

	return castedValue, nil
}

// GetDate gets the date value of the specified field.
// If the field is a group field, then its value can be
// obtained from the saved group value. Otherwise, the
// value is obtained from the appropriate aggregation function.
func (s *GroupByScan) GetDate(field string) (time.Time, error) {
	value, err := s.GetVal(field)
	if err != nil {
		return time.Time{}, err
	}

	castedValue, ok := value.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("field %s is not a date", field)
	}

	return castedValue, nil
}

// HasField returns true if the specified field is either a
// grouping field or created by an aggregation function.
func (s *GroupByScan) HasField(field string) bool {
	for _, groupField := range s.groupFields {
		if groupField == field {
			return true
		}
	}

	for _, function := range s.aggregationFunctions {
		if function.FieldName() == field {
			return true
		}
	}

	return false
}
