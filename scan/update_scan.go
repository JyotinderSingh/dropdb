package scan

import (
	"github.com/JyotinderSingh/dropdb/record"
	"time"
)

type UpdateScan interface {
	Scan

	// SetVal sets the value of the specified field in the current record.
	SetVal(fieldName string, val any) error

	// SetInt sets the integer value of the specified field in the current record.
	SetInt(fieldName string, val int) error

	// SetLong sets the long value of the specified field in the current record.
	SetLong(fieldName string, val int64) error

	// SetShort sets the short value of the specified field in the current record.
	SetShort(fieldName string, val int16) error

	// SetString sets the string value of the specified field in the current record.
	SetString(fieldName string, val string) error

	// SetBool sets the boolean value of the specified field in the current record.
	SetBool(fieldName string, val bool) error

	// SetDate sets the date value of the specified field in the current record.
	SetDate(fieldName string, val time.Time) error

	// Insert inserts a new record somewhere in the scan.
	Insert() error

	// Delete deletes the current record from the scan.
	Delete() error

	// GetRecordID returns the record ID of the current record.
	GetRecordID() *record.ID

	// MoveToRecordID moves the scan to the record with the specified record ID.
	MoveToRecordID(rid *record.ID) error
}
