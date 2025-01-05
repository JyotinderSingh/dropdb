package query

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"time"
)

var (
	_                     scan.UpdateScan = (*ProductScan)(nil)
	ErrUpdateNotSupported                 = "update not supported on scan: %T"
)

type ProductScan struct {
	scan1 scan.Scan
	scan2 scan.Scan
}

func NewProductScan(s1, s2 scan.Scan) *ProductScan {
	return &ProductScan{scan1: s1, scan2: s2}
}

// BeforeFirst positions the scan before its first record.
// In particular, the LHS scan is positioned at its first record,
// and the RHS scan is positioned before its first record.
func (ps *ProductScan) BeforeFirst() error {
	if err := ps.scan1.BeforeFirst(); err != nil {
		return err
	}
	if _, err := ps.scan1.Next(); err != nil {
		return err
	}
	return ps.scan2.BeforeFirst()
}

// Next moves the scan to the next record.
// The method moves to the next RHS record, if possible.
// Otherwise, it moves to the next LHS record and the first RHS record.
// If no more LHS records, the method returns false.
// Next tries s2 first. If s2 has a next record, return true.
// Otherwise, reset s2 to before the first record, then see if s2 and s1 each have a next record.
func (ps *ProductScan) Next() (bool, error) {
	// First try to advance s2
	hasNextS2, err := ps.scan2.Next()
	if err != nil {
		return false, err
	}
	if hasNextS2 {
		return true, nil
	}

	// If scan2 has no next record, reset it
	if err := ps.scan2.BeforeFirst(); err != nil {
		return false, err
	}

	// Now check both scan2 and s1
	hasNextS2, err = ps.scan2.Next()
	if err != nil || !hasNextS2 {
		return false, err
	}
	hasNextS1, err := ps.scan1.Next()
	if err != nil || !hasNextS1 {
		return false, err
	}

	return true, nil
}

// Close closes the scan.
func (ps *ProductScan) Close() {
	ps.scan1.Close()
	ps.scan2.Close()
}

// HasField returns true if the specified field is in either of the underlying scans.
func (ps *ProductScan) HasField(fieldName string) bool {
	return ps.scan1.HasField(fieldName) || ps.scan2.HasField(fieldName)
}

// GetInt returns the integer value of the specified field in the current record.
func (ps *ProductScan) GetInt(fieldName string) (int, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetInt(fieldName)
	}
	return ps.scan2.GetInt(fieldName)
}

// GetLong returns the long value of the specified field in the current record.
func (ps *ProductScan) GetLong(fieldName string) (int64, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetLong(fieldName)
	}
	return ps.scan2.GetLong(fieldName)
}

// GetShort returns the short value of the specified field in the current record.
func (ps *ProductScan) GetShort(fieldName string) (int16, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetShort(fieldName)
	}
	return ps.scan2.GetShort(fieldName)
}

// GetString returns the string value of the specified field in the current record.
func (ps *ProductScan) GetString(fieldName string) (string, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetString(fieldName)
	}
	return ps.scan2.GetString(fieldName)
}

// GetBool returns the boolean value of the specified field in the current record.
func (ps *ProductScan) GetBool(fieldName string) (bool, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetBool(fieldName)
	}
	return ps.scan2.GetBool(fieldName)
}

// GetDate returns the date value of the specified field in the current record.
func (ps *ProductScan) GetDate(fieldName string) (time.Time, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetDate(fieldName)
	}
	return ps.scan2.GetDate(fieldName)
}

// GetVal returns the value of the specified field in the current record.
func (ps *ProductScan) GetVal(fieldName string) (interface{}, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.GetVal(fieldName)
	}
	return ps.scan2.GetVal(fieldName)
}

// SetInt sets the integer value of the specified field in the current record.
func (ps *ProductScan) SetInt(fieldName string, val int) error {
	if ps.scan1.HasField(fieldName) {
		updateScan, ok := ps.scan1.(scan.UpdateScan)
		if !ok {
			return fmt.Errorf(ErrUpdateNotSupported, ps.scan1)
		}
		return updateScan.SetInt(fieldName, val)
	}
	updateScan, ok := ps.scan2.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.scan2)
	}
	return updateScan.SetInt(fieldName, val)
}

// SetLong sets the long value of the specified field in the current record.
func (ps *ProductScan) SetLong(fieldName string, val int64) error {
	if ps.scan1.HasField(fieldName) {
		updateScan, ok := ps.scan1.(scan.UpdateScan)
		if !ok {
			return fmt.Errorf(ErrUpdateNotSupported, ps.scan1)
		}
		return updateScan.SetLong(fieldName, val)
	}
	updateScan, ok := ps.scan2.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.scan2)
	}
	return updateScan.SetLong(fieldName, val)
}

// SetShort sets the short value of the specified field in the current record.
func (ps *ProductScan) SetShort(fieldName string, val int16) error {
	if ps.scan1.HasField(fieldName) {
		updateScan, ok := ps.scan1.(scan.UpdateScan)
		if !ok {
			return fmt.Errorf(ErrUpdateNotSupported, ps.scan1)
		}
		return updateScan.SetShort(fieldName, val)
	}
	updateScan, ok := ps.scan2.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.scan2)
	}
	return updateScan.SetShort(fieldName, val)
}

// SetString sets the string value of the specified field in the current record.
func (ps *ProductScan) SetString(fieldName string, val string) error {
	if ps.scan1.HasField(fieldName) {
		updateScan, ok := ps.scan1.(scan.UpdateScan)
		if !ok {
			return fmt.Errorf(ErrUpdateNotSupported, ps.scan1)
		}
		return updateScan.SetString(fieldName, val)
	}
	updateScan, ok := ps.scan2.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.scan2)
	}
	return updateScan.SetString(fieldName, val)
}

// SetBool sets the boolean value of the specified field in the current record.
func (ps *ProductScan) SetBool(fieldName string, val bool) error {
	if ps.scan1.HasField(fieldName) {
		updateScan, ok := ps.scan1.(scan.UpdateScan)
		if !ok {
			return fmt.Errorf(ErrUpdateNotSupported, ps.scan1)
		}
		return updateScan.SetBool(fieldName, val)
	}
	updateScan, ok := ps.scan2.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.scan2)
	}
	return updateScan.SetBool(fieldName, val)
}

// SetDate sets the date value of the specified field in the current record.
func (ps *ProductScan) SetDate(fieldName string, val time.Time) error {
	if ps.scan1.HasField(fieldName) {
		updateScan, ok := ps.scan1.(scan.UpdateScan)
		if !ok {
			return fmt.Errorf(ErrUpdateNotSupported, ps.scan1)
		}
		return updateScan.SetDate(fieldName, val)
	}
	updateScan, ok := ps.scan2.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.scan2)
	}
	return updateScan.SetDate(fieldName, val)
}

// SetVal sets the value of the specified field in the current record.
func (ps *ProductScan) SetVal(fieldName string, val interface{}) error {
	if ps.scan1.HasField(fieldName) {
		updateScan, ok := ps.scan1.(scan.UpdateScan)
		if !ok {
			return fmt.Errorf(ErrUpdateNotSupported, ps.scan1)
		}
		return updateScan.SetVal(fieldName, val)
	}
	updateScan, ok := ps.scan2.(scan.UpdateScan)
	if !ok {
		return fmt.Errorf(ErrUpdateNotSupported, ps.scan2)
	}
	return updateScan.SetVal(fieldName, val)
}

func (ps *ProductScan) Insert() error {
	// Insert not supported on ProductScan, this is because we can't insert a record into a product of two scans.
	return fmt.Errorf("insert not supported on ProductScan")
}

func (ps *ProductScan) Delete() error {
	// Delete not supported on ProductScan, this is because we can't delete a record from a product of two scans.
	return fmt.Errorf("delete not supported on ProductScan")
}

func (ps *ProductScan) GetRecordID() *record.ID {
	// GetRecordID not supported on ProductScan, this is because we can't get a record ID from a product of two scans.
	// Each row in the product is a combination of two records, so there is no single record ID.
	panic("GetRecordID not supported on ProductScan")
}

func (ps *ProductScan) MoveToRecordID(rid *record.ID) error {
	// MoveToRecordID not supported on ProductScan, this is because we can't move to a record ID in a product of two scans.
	// Each row in the product is a combination of two records, so there is no single record ID.
	return fmt.Errorf("MoveToRecordID not supported on ProductScan")
}
