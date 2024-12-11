package record

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/tx"
	"time"
)

const (
	FlagEmpty = iota
	FlagUsed
)

var (
	ErrNoSlotFound = fmt.Errorf("no slot found")
)

// Page stores a record at a given location in a block.
// A (record) page manages a block of records.
type Page struct {
	tx     *tx.Transaction
	block  *file.BlockId
	layout *Layout
}

// NewPage creates a new page.
func NewPage(transaction *tx.Transaction, block *file.BlockId, layout *Layout) (*Page, error) {
	if err := transaction.Pin(block); err != nil {
		return nil, err
	}
	return &Page{
		tx:     transaction,
		block:  block,
		layout: layout,
	}, nil
}

// GetInt returns the integer value stored for the specified field of a specified slot.
func (p *Page) GetInt(slot int, fieldName string) (int, error) {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.GetInt(p.block, fieldPosition)
}

// GetLong returns the long value stored for the specified field of a specified slot.
func (p *Page) GetLong(slot int, fieldName string) (int64, error) {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.GetLong(p.block, fieldPosition)
}

// GetString returns the string value stored for the specified field of a specified slot.
func (p *Page) GetString(slot int, fieldName string) (string, error) {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.GetString(p.block, fieldPosition)
}

// GetBool returns the boolean value stored for the specified field of a specified slot.
func (p *Page) GetBool(slot int, fieldName string) (bool, error) {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.GetBool(p.block, fieldPosition)
}

// GetDate returns the date value stored for the specified field of a specified slot.
func (p *Page) GetDate(slot int, fieldName string) (time.Time, error) {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.GetDate(p.block, fieldPosition)
}

// GetShort returns the short value stored for the specified field of a specified slot.
func (p *Page) GetShort(slot int, fieldName string) (int16, error) {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.GetShort(p.block, fieldPosition)
}

// SetInt stores an integer value for the specified field of a specified slot.
func (p *Page) SetInt(slot int, fieldName string, val int) error {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.SetInt(p.block, fieldPosition, val, true)
}

// SetLong stores a long value for the specified field of a specified slot.
func (p *Page) SetLong(slot int, fieldName string, val int64) error {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.SetLong(p.block, fieldPosition, val, true)
}

// SetString stores a string value for the specified field of a specified slot.
func (p *Page) SetString(slot int, fieldName string, val string) error {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.SetString(p.block, fieldPosition, val, true)
}

// SetBool stores a boolean value for the specified field of a specified slot.
func (p *Page) SetBool(slot int, fieldName string, val bool) error {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.SetBool(p.block, fieldPosition, val, true)
}

// SetDate stores a date value for the specified field of a specified slot.
func (p *Page) SetDate(slot int, fieldName string, val time.Time) error {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.SetDate(p.block, fieldPosition, val, true)
}

// SetShort stores a short value for the specified field of a specified slot.
func (p *Page) SetShort(slot int, fieldName string, val int16) error {
	fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)
	return p.tx.SetShort(p.block, fieldPosition, val, true)
}

// Delete marks a slot as empty.
func (p *Page) Delete(slot int) error {
	return p.setFlag(slot, FlagEmpty)
}

// Format uses the layout to format a new block of records.
// Note: These values are not logged (because the old values are meaningless).
func (p *Page) Format() error {
	slot := 0
	for p.isValidSlot(slot) {
		err := p.tx.SetInt(p.block, p.offset(slot), FlagEmpty, false)
		if err != nil {
			return err
		}

		schema := p.layout.Schema()

		for _, fieldName := range schema.Fields() {
			fieldPosition := p.offset(slot) + p.layout.Offset(fieldName)

			switch schema.Type(fieldName) {
			case Integer:
				err = p.tx.SetInt(p.block, fieldPosition, 0, false)
			case Long:
				err = p.tx.SetLong(p.block, fieldPosition, 0, false)
			case Short:
				err = p.tx.SetShort(p.block, fieldPosition, 0, false)
			case Boolean:
				err = p.tx.SetBool(p.block, fieldPosition, false, false)
			case Date:
				err = p.tx.SetDate(p.block, fieldPosition, time.Time{}, false)
			case Varchar:
				err = p.tx.SetString(p.block, fieldPosition, "", false)
			}

			if err != nil {
				return err
			}
		}
		slot++
	}
	return nil
}

// NextAfter returns the next slot that is in use after the specified slot.
func (p *Page) NextAfter(slot int) (int, error) {
	return p.searchAfter(slot, FlagUsed)
}

// InsertAfter inserts a new record after the specified slot and returns the new slot number.
// It performs the insertion by searching for the next empty slot.
func (p *Page) InsertAfter(slot int) (int, error) {
	newSlot, err := p.searchAfter(slot, FlagEmpty)
	if err != nil {
		return -1, fmt.Errorf("insert after slot %d: %w", slot, err)
	}

	if err := p.setFlag(newSlot, FlagUsed); err != nil {
		return -1, fmt.Errorf("set flag for slot %d: %w", newSlot, err)
	}
	return newSlot, nil
}

// searchAfter finds the next slot with the specified flag. It returns the slot number.
// If no slot is found, it returns an error.
func (p *Page) searchAfter(slot, flag int) (int, error) {
	slot++ // Move to next slot

	for p.isValidSlot(slot) {
		currentFlag, err := p.tx.GetInt(p.block, p.offset(slot))
		if err != nil {
			return -1, fmt.Errorf("read flag at slot %d: %w", slot, err)
		}

		if currentFlag == flag {
			return slot, nil
		}
		slot++
	}
	return -1, ErrNoSlotFound
}

// Block returns the block that the page is using.
func (p *Page) Block() *file.BlockId {
	return p.block
}

// isValidSlot returns true if the slot is within the block's capacity.
func (p *Page) isValidSlot(slot int) bool {
	return p.offset(slot+1) <= p.tx.BlockSize()
}

// offset returns the offset of the specified slot.
// The offset is the number of bytes from the start of the block to the start of the slot.
func (p *Page) offset(slot int) int {
	return slot * p.layout.SlotSize()
}

// setFlag sets the record's empty/in-use flag.
func (p *Page) setFlag(slot int, flag int) error {
	return p.tx.SetInt(p.block, p.offset(slot), flag, true)
}
