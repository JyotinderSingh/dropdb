package query

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"time"
)

var (
	_                scan.UpdateScan = (*ProjectScan)(nil)
	ErrFieldNotFound                 = "field %s not found"
)

type ProjectScan struct {
	inputScan scan.Scan
	fieldList []string
}

func NewProjectScan(s scan.Scan, fieldList []string) (*ProjectScan, error) {
	return &ProjectScan{inputScan: s, fieldList: fieldList}, nil
}

func (ps *ProjectScan) BeforeFirst() error {
	return ps.inputScan.BeforeFirst()
}

func (ps *ProjectScan) Next() (bool, error) {
	return ps.inputScan.Next()
}

func (ps *ProjectScan) Close() {
	ps.inputScan.Close()
}

// HasField returns true if the specified field is in the field list.
func (ps *ProjectScan) HasField(fieldName string) bool {
	for _, f := range ps.fieldList {
		if f == fieldName {
			return true
		}
	}
	return false
}

// GetInt returns the integer value of the specified field in the current record.
func (ps *ProjectScan) GetInt(fieldName string) (int, error) {
	if !ps.HasField(fieldName) {
		return 0, fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	return ps.inputScan.GetInt(fieldName)
}

// GetLong returns the long value of the specified field in the current record.
func (ps *ProjectScan) GetLong(fieldName string) (int64, error) {
	if !ps.HasField(fieldName) {
		return 0, fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	return ps.inputScan.GetLong(fieldName)
}

// GetShort returns the short value of the specified field in the current record.
func (ps *ProjectScan) GetShort(fieldName string) (int16, error) {
	if !ps.HasField(fieldName) {
		return 0, fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	return ps.inputScan.GetShort(fieldName)
}

// GetString returns the string value of the specified field in the current record.
func (ps *ProjectScan) GetString(fieldName string) (string, error) {
	if !ps.HasField(fieldName) {
		return "", fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	return ps.inputScan.GetString(fieldName)
}

// GetBool returns the boolean value of the specified field in the current record.
func (ps *ProjectScan) GetBool(fieldName string) (bool, error) {
	if !ps.HasField(fieldName) {
		return false, fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	return ps.inputScan.GetBool(fieldName)
}

// GetDate returns the date value of the specified field in the current record.
func (ps *ProjectScan) GetDate(fieldName string) (time.Time, error) {
	if !ps.HasField(fieldName) {
		return time.Time{}, fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	return ps.inputScan.GetDate(fieldName)
}

// GetVal returns the value of the specified field in the current record.
func (ps *ProjectScan) GetVal(fieldName string) (interface{}, error) {
	if !ps.HasField(fieldName) {
		return nil, fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	return ps.inputScan.GetVal(fieldName)
}

// SetInt sets the integer value of the specified field in the current record.
func (ps *ProjectScan) SetInt(fieldName string, val int) error {
	if !ps.HasField(fieldName) {
		return fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.SetInt(fieldName, val)
}

// SetLong sets the long value of the specified field in the current record.
func (ps *ProjectScan) SetLong(fieldName string, val int64) error {
	if !ps.HasField(fieldName) {
		return fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.SetLong(fieldName, val)
}

// SetShort sets the short value of the specified field in the current record.
func (ps *ProjectScan) SetShort(fieldName string, val int16) error {
	if !ps.HasField(fieldName) {
		return fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.SetShort(fieldName, val)
}

// SetString sets the string value of the specified field in the current record.
func (ps *ProjectScan) SetString(fieldName string, val string) error {
	if !ps.HasField(fieldName) {
		return fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.SetString(fieldName, val)
}

// SetBool sets the boolean value of the specified field in the current record.
func (ps *ProjectScan) SetBool(fieldName string, val bool) error {
	if !ps.HasField(fieldName) {
		return fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.SetBool(fieldName, val)
}

// SetDate sets the date value of the specified field in the current record.
func (ps *ProjectScan) SetDate(fieldName string, val time.Time) error {
	if !ps.HasField(fieldName) {
		return fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.SetDate(fieldName, val)
}

// SetVal sets the value of the specified field in the current record.
func (ps *ProjectScan) SetVal(fieldName string, val interface{}) error {
	if !ps.HasField(fieldName) {
		return fmt.Errorf(ErrFieldNotFound, fieldName)
	}
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.SetVal(fieldName, val)
}

// Insert inserts a new record somewhere in the scan.
func (ps *ProjectScan) Insert() error {
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.Insert()
}

// Delete deletes the current record from the scan.
func (ps *ProjectScan) Delete() error {
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.Delete()
}

// GetRecordID returns the record ID of the current record.
func (ps *ProjectScan) GetRecordID() *record.ID {
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		panic(fmt.Sprintf(ErrUpdateNotSupported, ps.inputScan))
	}
	return updateScan.GetRecordID()
}

// MoveToRecordID moves the scan to the record with the specified record ID.
func (ps *ProjectScan) MoveToRecordID(rid *record.ID) error {
	updateScan, ok := ps.inputScan.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.inputScan)
	}
	return updateScan.MoveToRecordID(rid)
}
