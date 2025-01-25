package query

import (
	"github.com/JyotinderSingh/dropdb/index"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/table"
	"time"
)

var _ scan.Scan = (*IndexJoinScan)(nil)

// IndexJoinScan is a scan that joins two scans using an index.
// It uses the index to look up the right-hand side of the join for each row of the left-hand side.
type IndexJoinScan struct {
	lhs       scan.Scan
	rhs       *table.Scan
	joinField string
	idx       index.Index
}

// NewIndexJoinScan creates a new IndexJoinScan for the specified LHS scan and RHS index.
func NewIndexJoinScan(lhs scan.Scan, rhs *table.Scan, joinField string, idx index.Index) (*IndexJoinScan, error) {
	ijs := &IndexJoinScan{
		lhs:       lhs,
		rhs:       rhs,
		joinField: joinField,
		idx:       idx,
	}

	if err := ijs.BeforeFirst(); err != nil {
		return nil, err
	}

	return ijs, nil
}

// BeforeFirst resets the scan and positions it before the first record.
// That is, the LHS scan will be positioned at its first record, and
// the RHS scan will be positioned at the first record for the join value.
func (ijs *IndexJoinScan) BeforeFirst() error {
	if err := ijs.lhs.BeforeFirst(); err != nil {
		return err
	}

	if _, err := ijs.lhs.Next(); err != nil {
		return err
	}

	return ijs.resetIndex()
}

// Next advances the scan to the next record.
// The method moves to the next index record, if possible.
// Otherwise, it moves to the next LHS record and the
// first index record.
func (ijs *IndexJoinScan) Next() (bool, error) {
	for {
		hasNext, err := ijs.idx.Next()
		if err != nil {
			return false, err
		}

		if hasNext {
			recordID, err := ijs.idx.GetDataRecordID()
			if err != nil {
				return false, err
			}
			if err := ijs.rhs.MoveToRecordID(recordID); err != nil {
				return false, err
			}
			return true, nil
		}

		hasNext, err = ijs.lhs.Next()
		if err != nil {
			return false, err
		}
		if !hasNext {
			return false, nil
		}

		if err := ijs.resetIndex(); err != nil {
			return false, err
		}
	}
}

// GetInt returns the integer value of the specified field in the current record.
func (ijs *IndexJoinScan) GetInt(fieldName string) (int, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetInt(fieldName)
	}
	return ijs.lhs.GetInt(fieldName)
}

// GetLong returns the long value of the specified field in the current record.
func (ijs *IndexJoinScan) GetLong(fieldName string) (int64, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetLong(fieldName)
	}
	return ijs.lhs.GetLong(fieldName)
}

// GetShort returns the short value of the specified field in the current record.
func (ijs *IndexJoinScan) GetShort(fieldName string) (int16, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetShort(fieldName)
	}
	return ijs.lhs.GetShort(fieldName)
}

// GetString returns the string value of the specified field in the current record.
func (ijs *IndexJoinScan) GetString(fieldName string) (string, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetString(fieldName)
	}
	return ijs.lhs.GetString(fieldName)
}

// GetBool returns the boolean value of the specified field in the current record.
func (ijs *IndexJoinScan) GetBool(fieldName string) (bool, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetBool(fieldName)
	}
	return ijs.lhs.GetBool(fieldName)
}

// GetDate returns the date value of the specified field in the current record.
func (ijs *IndexJoinScan) GetDate(fieldName string) (time.Time, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetDate(fieldName)
	}
	return ijs.lhs.GetDate(fieldName)
}

// GetVal returns the value of the specified field in the current record.
func (ijs *IndexJoinScan) GetVal(fieldName string) (any, error) {
	if ijs.rhs.HasField(fieldName) {
		return ijs.rhs.GetVal(fieldName)
	}
	return ijs.lhs.GetVal(fieldName)

}

// HasField returns true if the field is in the schema.
func (ijs *IndexJoinScan) HasField(fieldName string) bool {
	return ijs.lhs.HasField(fieldName) || ijs.rhs.HasField(fieldName)
}

// Close closes the scan and its subscans.
func (ijs *IndexJoinScan) Close() {
	ijs.lhs.Close()
	ijs.rhs.Close()
	ijs.idx.Close()
}

func (ijs *IndexJoinScan) resetIndex() error {
	searchKey, err := ijs.lhs.GetVal(ijs.joinField)
	if err != nil {
		return err
	}

	return ijs.idx.BeforeFirst(searchKey)
}
