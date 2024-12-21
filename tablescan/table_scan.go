package tablescan

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"time"
)

const fileExtension = ".tbl"

// TableScan provides the abstraction of an arbitrarily large array of records.
type TableScan struct {
	query.UpdateScan
	tx          *tx.Transaction
	layout      *record.Layout
	recordPage  *record.Page
	fileName    string
	currentSlot int
}

// NewTableScan creates a new table scan
func NewTableScan(tx *tx.Transaction, tableName string, layout *record.Layout) (*TableScan, error) {
	ts := &TableScan{
		tx:          tx,
		layout:      layout,
		fileName:    tableName + fileExtension,
		currentSlot: -1,
	}

	size, err := tx.Size(ts.fileName)
	if err != nil {
		return nil, fmt.Errorf("get file size: %w", err)
	}

	if size == 0 {
		if err := ts.moveToNewBlock(); err != nil {
			return nil, fmt.Errorf("move to new block: %w", err)
		}
	} else {
		if err := ts.moveToBlock(0); err != nil {
			return nil, fmt.Errorf("move to block 0: %w", err)
		}
	}

	return ts, nil
}

func (ts *TableScan) BeforeFirst() error {
	return ts.moveToBlock(0)
}

func (ts *TableScan) Next() (bool, error) {
	slot, err := ts.recordPage.NextAfter(ts.currentSlot)

	if err != nil {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return false, err
		}
		if atLastBlock {
			return false, nil
		}
		if err := ts.moveToBlock(ts.recordPage.Block().Number() + 1); err != nil {
			return false, err
		}
		slot, err = ts.recordPage.NextAfter(-1)
		if err != nil {
			return false, nil
		}
	}

	ts.currentSlot = slot
	return true, nil
}

func (ts *TableScan) GetInt(fieldName string) (int, error) {
	return ts.recordPage.GetInt(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetLong(fieldName string) (int64, error) {
	return ts.recordPage.GetLong(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetShort(fieldName string) (int16, error) {
	return ts.recordPage.GetShort(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetString(fieldName string) (string, error) {
	return ts.recordPage.GetString(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetBool(fieldName string) (bool, error) {
	return ts.recordPage.GetBool(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetDate(fieldName string) (time.Time, error) {
	return ts.recordPage.GetDate(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetVal(fieldName string) (query.Constant, error) {
	fieldType := ts.layout.Schema().Type(fieldName)

	switch fieldType {
	case record.Integer:
		val, err := ts.GetInt(fieldName)
		return query.NewConstant(val), err
	case record.Long:
		val, err := ts.GetLong(fieldName)
		return query.NewConstant(val), err
	case record.Short:
		val, err := ts.GetShort(fieldName)
		return query.NewConstant(val), err
	case record.Varchar:
		val, err := ts.GetString(fieldName)
		return query.NewConstant(val), err
	case record.Boolean:
		val, err := ts.GetBool(fieldName)
		return query.NewConstant(val), err
	case record.Date:
		val, err := ts.GetDate(fieldName)
		return query.NewConstant(val), err
	default:
		return query.Constant{}, fmt.Errorf("unsupported field type: %v", fieldType)
	}
}

func (ts *TableScan) SetInt(fieldName string, val int) error {
	return ts.recordPage.SetInt(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetLong(fieldName string, val int64) error {
	return ts.recordPage.SetLong(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetShort(fieldName string, val int16) error {
	return ts.recordPage.SetShort(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetString(fieldName string, val string) error {
	return ts.recordPage.SetString(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetBool(fieldName string, val bool) error {
	return ts.recordPage.SetBool(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetDate(fieldName string, val time.Time) error {
	return ts.recordPage.SetDate(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetVal(fieldName string, val query.Constant) error {
	switch ts.layout.Schema().Type(fieldName) {
	case record.Integer:
		if v, ok := val.AsInt(); ok {
			return ts.SetInt(fieldName, v)
		}
	case record.Long:
		if v, ok := val.AsLong(); ok {
			return ts.SetLong(fieldName, v)
		}
	case record.Short:
		if v, ok := val.AsShort(); ok {
			return ts.SetShort(fieldName, v)
		}
	case record.Varchar:
		if v, ok := val.AsString(); ok {
			return ts.SetString(fieldName, v)
		}
	case record.Boolean:
		if v, ok := val.AsBool(); ok {
			return ts.SetBool(fieldName, v)
		}
	case record.Date:
		if v, ok := val.AsDate(); ok {
			return ts.SetDate(fieldName, v)
		}
	}
	return fmt.Errorf("type mismatch for field %s", fieldName)
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) Close() error {
	if ts.recordPage != nil {
		ts.tx.Unpin(ts.recordPage.Block())
	}
	return nil
}

func (ts *TableScan) Insert() error {
	slot, err := ts.recordPage.InsertAfter(ts.currentSlot)

	// Key change: match Java's behavior for handling InsertAfter
	if err != nil {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return fmt.Errorf("checking last block: %w", err)
		}

		if atLastBlock {
			if err := ts.moveToNewBlock(); err != nil {
				return fmt.Errorf("move to new block: %w", err)
			}
		} else {
			if err := ts.moveToBlock(ts.recordPage.Block().Number() + 1); err != nil {
				return fmt.Errorf("move to next block: %w", err)
			}
		}

		slot, err = ts.recordPage.InsertAfter(-1) // Start from beginning of new block
		if err != nil {
			return fmt.Errorf("insert in new block: %w", err)
		}
	}

	ts.currentSlot = slot
	return nil
}

func (ts *TableScan) Delete() error {
	return ts.recordPage.Delete(ts.currentSlot)
}

func (ts *TableScan) GetRecordID() *record.ID {
	return record.NewID(ts.recordPage.Block().Number(), ts.currentSlot)
}

func (ts *TableScan) MoveToRecordID(rid record.ID) error {
	if err := ts.Close(); err != nil {
		return fmt.Errorf("close current page: %w", err)
	}

	blk := &file.BlockId{
		File:        ts.fileName,
		BlockNumber: rid.BlockNumber(),
	}

	page, err := record.NewPage(ts.tx, blk, ts.layout)
	if err != nil {
		return fmt.Errorf("create new page: %w", err)
	}

	ts.recordPage = page
	ts.currentSlot = rid.Slot()
	return nil
}

// Private helper methods

// moveToBlock moves the scan to the specified block number.
func (ts *TableScan) moveToBlock(blockNum int) error {
	if err := ts.Close(); err != nil {
		return fmt.Errorf("close current page: %w", err)
	}

	blk := &file.BlockId{
		File:        ts.fileName,
		BlockNumber: blockNum,
	}

	page, err := record.NewPage(ts.tx, blk, ts.layout)
	if err != nil {
		return fmt.Errorf("create new page: %w", err)
	}

	ts.recordPage = page
	ts.currentSlot = -1
	return nil
}

// moveToNewBlock moves the scan to a new block.
func (ts *TableScan) moveToNewBlock() error {
	if err := ts.Close(); err != nil {
		return fmt.Errorf("close current page: %w", err)
	}

	blk, err := ts.tx.Append(ts.fileName)
	if err != nil {
		return fmt.Errorf("append block: %w", err)
	}

	page, err := record.NewPage(ts.tx, blk, ts.layout)
	if err != nil {
		return fmt.Errorf("create new page: %w", err)
	}

	if err := page.Format(); err != nil {
		return fmt.Errorf("format page: %w", err)
	}

	ts.recordPage = page
	ts.currentSlot = -1
	return nil
}

// atLastBlock returns true if the scan is at the last block.
func (ts *TableScan) atLastBlock() (bool, error) {
	fileSize, err := ts.tx.Size(ts.fileName)
	if err != nil {
		return false, fmt.Errorf("get file size: %w", err)
	}
	return ts.recordPage.Block().Number() == fileSize-1, nil
}
