package query

import "time"

// Scan interface will be implemented by each query scan.
// There is a Scan class for each relational algebra operator.
type Scan interface {
	// BeforeFirst positions the scan before the first record. A subsequent call to Next will move to the first record.
	BeforeFirst() error

	// Next moves to the next record in the scan. It returns false if there are no more records to scan.
	Next() (bool, error)

	// GetInt returns the integer value of the specified field in the current record.
	GetInt(fieldName string) (int, error)

	// GetLong returns the long value of the specified field in the current record.
	GetLong(fieldName string) (int64, error)

	// GetShort returns the short value of the specified field in the current record.
	GetShort(fieldName string) (int16, error)

	// GetString returns the string value of the specified field in the current record.
	GetString(fieldName string) (string, error)

	// GetBool returns the boolean value of the specified field in the current record.
	GetBool(fieldName string) (bool, error)

	// GetDate returns the date value of the specified field in the current record.
	GetDate(fieldName string) (time.Time, error)

	// HasField returns true if the current record has the specified field.
	HasField(fieldName string) bool

	// GetVal returns the value of the specified field in the current record.
	GetVal(fieldName string) (any, error)

	// Close closes the scan and its subscans, if any.
	Close()
}
