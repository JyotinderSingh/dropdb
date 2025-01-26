package query

import (
	"github.com/JyotinderSingh/dropdb/materialize"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"time"
)

var _ scan.Scan = &SortScan{}

// SortScan is a scan for the sort operator.
type SortScan struct {
	scan1         scan.UpdateScan
	scan2         scan.UpdateScan
	currentScan   scan.UpdateScan
	comparator    *RecordComparator
	hasMore1      bool
	hasMore2      bool
	savedPosition []*record.ID
}

// NewSortScan creates a sort scan, given a list of one or two sorted runs.
// If there is only one run, then s2 will be null and
// hasMore2 will be false.
func NewSortScan(runs []*materialize.TempTable, comparator *RecordComparator) (*SortScan, error) {
	ss := &SortScan{
		scan2:       nil,
		currentScan: nil,
		comparator:  comparator,
		hasMore2:    false,
	}

	var err error
	ss.scan1, err = runs[0].Open()
	if err != nil {
		return nil, err
	}

	ss.hasMore1, err = ss.scan1.Next()
	if err != nil {
		return nil, err
	}

	if len(runs) > 1 {
		ss.scan2, err = runs[1].Open()
		if err != nil {
			return nil, err
		}
		ss.hasMore2, err = ss.scan2.Next()
		if err != nil {
			return nil, err
		}
	}

	return ss, nil
}

// BeforeFirst positions the scan before the first record in sorted order.
// Internally, it moves to the first record of each underlying scan.
// The variable currentScan is set to nil, indicating that there is no current scan.
func (ss *SortScan) BeforeFirst() error {
	ss.currentScan = nil
	var err error

	if err = ss.scan1.BeforeFirst(); err != nil {
		return err
	}

	ss.hasMore1, err = ss.scan1.Next()
	if err != nil {
		return err
	}

	if ss.scan2 != nil {
		if err = ss.scan2.BeforeFirst(); err != nil {
			return err
		}
		ss.hasMore2, err = ss.scan2.Next()
		if err != nil {
			return err
		}
	}

	return nil
}

// Next moves to the next record in sorted order.
// First, the current scan is moved to the next record.
// Then the lowest record of the two scans is found,
// and that scan is chosen to be the new current scan.
func (ss *SortScan) Next() (bool, error) {
	// Advance the current scan if it exists
	if ss.currentScan != nil {
		var err error
		if ss.currentScan == ss.scan1 {
			ss.hasMore1, err = ss.scan1.Next()
		} else if ss.currentScan == ss.scan2 {
			ss.hasMore2, err = ss.scan2.Next()
		}
		if err != nil {
			return false, err
		}
	}

	// Determine if there are more records
	if !ss.hasMore1 && !ss.hasMore2 {
		return false, nil
	}

	// Choose the scan with the lowest record
	switch {
	case ss.hasMore1 && ss.hasMore2:
		if ss.comparator.Compare(ss.scan1, ss.scan2) < 0 {
			ss.currentScan = ss.scan1
		} else {
			ss.currentScan = ss.scan2
		}
	case ss.hasMore1:
		ss.currentScan = ss.scan1
	case ss.hasMore2:
		ss.currentScan = ss.scan2
	}

	return true, nil
}

// Close closes the two underlying scans.
func (ss *SortScan) Close() {
	ss.scan1.Close()

	if ss.scan2 != nil {
		ss.scan2.Close()
	}
}

// GetInt returns the integer value of the specified field in the current record.
func (ss *SortScan) GetInt(fieldName string) (int, error) { return ss.currentScan.GetInt(fieldName) }

// GetLong returns the long value of the specified field in the current record.
func (ss *SortScan) GetLong(fieldName string) (int64, error) {
	return ss.currentScan.GetLong(fieldName)
}

// GetShort returns the short value of the specified field in the current record.
func (ss *SortScan) GetShort(fieldName string) (int16, error) {
	return ss.currentScan.GetShort(fieldName)
}

// GetString returns the string value of the specified field in the current record.
func (ss *SortScan) GetString(fieldName string) (string, error) {
	return ss.currentScan.GetString(fieldName)
}

// GetBool returns the boolean value of the specified field in the current record.
func (ss *SortScan) GetBool(fieldName string) (bool, error) { return ss.currentScan.GetBool(fieldName) }

// GetDate returns the date value of the specified field in the current record.
func (ss *SortScan) GetDate(fieldName string) (time.Time, error) {
	return ss.currentScan.GetDate(fieldName)
}

// HasField returns true if the current record has the specified field.
func (ss *SortScan) HasField(fieldName string) bool { return ss.currentScan.HasField(fieldName) }

// GetVal returns the value of the specified field in the current record.
func (ss *SortScan) GetVal(fieldName string) (any, error) { return ss.currentScan.GetVal(fieldName) }

// GetRecordID returns the record ID of the current record.
func (ss *SortScan) GetRecordID() *record.ID { return ss.currentScan.GetRecordID() }

// SavePosition saves the position of the current record so that it can be restored at a later time.
func (ss *SortScan) SavePosition() {
	recordID1 := ss.scan1.GetRecordID()

	var recordID2 *record.ID
	if ss.scan2 != nil {
		recordID2 = ss.scan2.GetRecordID()
	}

	ss.savedPosition = []*record.ID{recordID1, recordID2}
}

// RestorePosition restores the position of the current record to the last saved position.
func (ss *SortScan) RestorePosition() error {
	recordID1 := ss.savedPosition[0]
	recordID2 := ss.savedPosition[1]

	if err := ss.scan1.MoveToRecordID(recordID1); err != nil {
		return err
	}

	if ss.scan2 != nil {
		if err := ss.scan2.MoveToRecordID(recordID2); err != nil {
			return err
		}
	}

	return nil
}
