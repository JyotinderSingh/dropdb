package table

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/types"
	"time"
)

const fileExtension = ".tbl"

// Ensure TableScan implements the UpdateScan interface.
var _ scan.UpdateScan = (*Scan)(nil)

// Scan provides the abstraction of an arbitrarily large array of records.
type Scan struct {
	tx          *tx.Transaction
	layout      *record.Layout
	recordPage  *record.Page
	fileName    string
	currentSlot int
}

// NewTableScan creates a new table scan
func NewTableScan(tx *tx.Transaction, tableName string, layout *record.Layout) (*Scan, error) {
	if layout.SlotSize() > tx.BlockSize() {
		return nil, fmt.Errorf("record slot size (%d) exceeds block size (%d)", layout.SlotSize(), tx.BlockSize())
	}

	ts := &Scan{
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

func (ts *Scan) BeforeFirst() error {
	return ts.moveToBlock(0)
}

// Next moves the scan to the next record in the table.
// It returns false if there are no more records to scan.
// Internally, it moves to the next slot in the current block.
// If there are no more slots in the block, it moves to the next block.
// If there are no more blocks, it returns false.
func (ts *Scan) Next() (bool, error) {
	slot, err := ts.recordPage.NextAfter(ts.currentSlot)

	if err != nil {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return false, err
		}
		if atLastBlock {
			return false, nil
		}
		// Move to the next block in the file, and load it into the record page. This will also move to the first slot.
		if err := ts.moveToBlock(ts.recordPage.Block().Number() + 1); err != nil {
			return false, err
		}
		if slot, err = ts.recordPage.NextAfter(ts.currentSlot); err != nil {
			return false, err
		}
	}

	ts.currentSlot = slot
	return true, nil
}

func (ts *Scan) GetInt(fieldName string) (int, error) {
	return ts.recordPage.GetInt(ts.currentSlot, fieldName)
}

func (ts *Scan) GetLong(fieldName string) (int64, error) {
	return ts.recordPage.GetLong(ts.currentSlot, fieldName)
}

func (ts *Scan) GetShort(fieldName string) (int16, error) {
	return ts.recordPage.GetShort(ts.currentSlot, fieldName)
}

func (ts *Scan) GetString(fieldName string) (string, error) {
	return ts.recordPage.GetString(ts.currentSlot, fieldName)
}

func (ts *Scan) GetBool(fieldName string) (bool, error) {
	return ts.recordPage.GetBool(ts.currentSlot, fieldName)
}

func (ts *Scan) GetDate(fieldName string) (time.Time, error) {
	return ts.recordPage.GetDate(ts.currentSlot, fieldName)
}

func (ts *Scan) GetVal(fieldName string) (any, error) {
	fieldType := ts.layout.Schema().Type(fieldName)

	switch fieldType {
	case types.Integer:
		val, err := ts.GetInt(fieldName)
		return val, err
	case types.Long:
		val, err := ts.GetLong(fieldName)
		return val, err
	case types.Short:
		val, err := ts.GetShort(fieldName)
		return val, err
	case types.Varchar:
		val, err := ts.GetString(fieldName)
		return val, err
	case types.Boolean:
		val, err := ts.GetBool(fieldName)
		return val, err
	case types.Date:
		val, err := ts.GetDate(fieldName)
		return val, err
	default:
		return nil, fmt.Errorf("unsupported field type: %v", fieldType)
	}
}

func (ts *Scan) SetInt(fieldName string, val int) error {
	return ts.recordPage.SetInt(ts.currentSlot, fieldName, val)
}

func (ts *Scan) SetLong(fieldName string, val int64) error {
	return ts.recordPage.SetLong(ts.currentSlot, fieldName, val)
}

func (ts *Scan) SetShort(fieldName string, val int16) error {
	return ts.recordPage.SetShort(ts.currentSlot, fieldName, val)
}

func (ts *Scan) SetString(fieldName string, val string) error {
	return ts.recordPage.SetString(ts.currentSlot, fieldName, val)
}

func (ts *Scan) SetBool(fieldName string, val bool) error {
	return ts.recordPage.SetBool(ts.currentSlot, fieldName, val)
}

func (ts *Scan) SetDate(fieldName string, val time.Time) error {
	return ts.recordPage.SetDate(ts.currentSlot, fieldName, val)
}

func (ts *Scan) SetVal(fieldName string, val any) error {
	switch ts.layout.Schema().Type(fieldName) {
	case types.Integer:
		if v, ok := val.(int); ok {
			return ts.SetInt(fieldName, v)
		}
	case types.Long:
		if v, ok := val.(int64); ok {
			return ts.SetLong(fieldName, v)
		}
	case types.Short:
		if v, ok := val.(int16); ok {
			return ts.SetShort(fieldName, v)
		}
	case types.Varchar:
		if v, ok := val.(string); ok {
			return ts.SetString(fieldName, v)
		}
	case types.Boolean:
		if v, ok := val.(bool); ok {
			return ts.SetBool(fieldName, v)
		}
	case types.Date:
		if v, ok := val.(time.Time); ok {
			return ts.SetDate(fieldName, v)
		}
	}
	return fmt.Errorf("type mismatch for field %s", fieldName)
}

func (ts *Scan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

// Close closes the scan.
// Unpins the current record page.
func (ts *Scan) Close() {
	if ts.recordPage != nil {
		ts.tx.Unpin(ts.recordPage.Block())
	}
}

// Insert inserts a new record somewhere in the scan and moves the scan to the new record.
// If there is no room in the current block, it moves to the next block.
// If there are no more blocks, it creates a new block.
func (ts *Scan) Insert() error {
	if ts.layout.SlotSize() > ts.tx.BlockSize() {
		return fmt.Errorf("record slot size (%d) exceeds block size (%d)", ts.layout.SlotSize(), ts.tx.BlockSize())
	}

	for {
		slot, err := ts.recordPage.InsertAfter(ts.currentSlot)
		if err == nil {
			// Successfully inserted
			ts.currentSlot = slot
			return nil
		}

		// Check if we are at the last block.
		atLastBlock, err2 := ts.atLastBlock()
		if err2 != nil {
			return fmt.Errorf("checking last block: %w", err2)
		}

		if atLastBlock {
			// If it's the last block, append a new block and format it.
			if err := ts.moveToNewBlock(); err != nil {
				return fmt.Errorf("move to new block: %w", err)
			}
		} else {
			// Otherwise, move to the next block in the file and try again.
			nextBlockNum := ts.recordPage.Block().Number() + 1
			if err := ts.moveToBlock(nextBlockNum); err != nil {
				return fmt.Errorf("move to next block: %w", err)
			}
		}

		// We’ll loop again and try the InsertAfter in the newly pinned page.
	}
}

func (ts *Scan) Delete() error {
	return ts.recordPage.Delete(ts.currentSlot)
}

func (ts *Scan) GetRecordID() *record.ID {
	return record.NewID(ts.recordPage.Block().Number(), ts.currentSlot)
}

func (ts *Scan) MoveToRecordID(rid *record.ID) error {
	ts.Close()

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
func (ts *Scan) moveToBlock(blockNum int) error {
	ts.Close()

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

// moveToNewBlock moves the scan to a new block. It appends a new block to the file and loads it into the record page.
func (ts *Scan) moveToNewBlock() error {
	ts.Close()

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
func (ts *Scan) atLastBlock() (bool, error) {
	fileSize, err := ts.tx.Size(ts.fileName)
	if err != nil {
		return false, fmt.Errorf("get file size: %w", err)
	}
	return ts.recordPage.Block().Number() == fileSize-1, nil
}
