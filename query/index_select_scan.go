package query

import (
	"github.com/JyotinderSingh/dropdb/index"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/table"
	"time"
)

var _ scan.Scan = (*IndexSelectScan)(nil)

// IndexSelectScan is a scan that combines an index scan with a table scan.
// It is used to scan the data records of a table that satisfy a selection
// constant on an index.
type IndexSelectScan struct {
	tableScan *table.Scan
	idx       index.Index
	value     any
}

// NewIndexSelectScan creates an index select scan for the specified index
// and selection constant.
func NewIndexSelectScan(tableScan *table.Scan, idx index.Index, value any) (*IndexSelectScan, error) {
	iss := &IndexSelectScan{
		tableScan: tableScan,
		idx:       idx,
		value:     value,
	}
	if err := iss.BeforeFirst(); err != nil {
		return nil, err
	}
	return iss, nil
}

// BeforeFirst positions the scan before the first record,
// which in this case means positioning the index before
// the first instance of the selection constant.
func (iss *IndexSelectScan) BeforeFirst() error {
	return iss.idx.BeforeFirst(iss.value)
}

// Next moves to the next record, which in this case means
// moving the index to the next record satisfying the
// selection constant, and returning false if there are no
// more such index records.
// If there is a next record, the method moves the tablescan
// to the corresponding data record.
func (iss *IndexSelectScan) Next() (bool, error) {
	next, err := iss.idx.Next()
	if !next || err != nil {
		return next, err
	}
	dataRID, err := iss.idx.GetDataRecordID()
	if err != nil {
		return false, err
	}
	return next, iss.tableScan.MoveToRecordID(dataRID)
}

// GetInt returns the integer value of the specified field in the current record.
func (iss *IndexSelectScan) GetInt(fieldName string) (int, error) {
	return iss.tableScan.GetInt(fieldName)
}

// GetLong returns the long value of the specified field in the current record.
func (iss *IndexSelectScan) GetLong(fieldName string) (int64, error) {
	return iss.tableScan.GetLong(fieldName)
}

// GetShort returns the short value of the specified field in the current record.
func (iss *IndexSelectScan) GetShort(fieldName string) (int16, error) {
	return iss.tableScan.GetShort(fieldName)
}

// GetString returns the string value of the specified field in the current record.
func (iss *IndexSelectScan) GetString(fieldName string) (string, error) {
	return iss.tableScan.GetString(fieldName)
}

// GetBool returns the boolean value of the specified field in the current record.
func (iss *IndexSelectScan) GetBool(fieldName string) (bool, error) {
	return iss.tableScan.GetBool(fieldName)
}

// GetDate returns the date value of the specified field in the current record.
func (iss *IndexSelectScan) GetDate(fieldName string) (time.Time, error) {
	return iss.tableScan.GetDate(fieldName)
}

// GetVal returns the value of the specified field in the current record.
func (iss *IndexSelectScan) GetVal(fieldName string) (any, error) {
	return iss.tableScan.GetVal(fieldName)
}

// HasField returns true if the underlying scan has the specified field.
func (iss *IndexSelectScan) HasField(fieldName string) bool {
	return iss.tableScan.HasField(fieldName)
}

// Close closes the scan by closing the index and the tablescan.
func (iss *IndexSelectScan) Close() {
	iss.idx.Close()
	iss.tableScan.Close()
}
