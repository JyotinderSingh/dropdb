package tx

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/utils"
)

type SetStringRecord struct {
	LogRecord
	txNum  int
	offset int
	value  string
	block  *file.BlockId
}

// NewSetStringRecord creates a new SetStringRecord from a Page.
func NewSetStringRecord(page *file.Page) (*SetStringRecord, error) {
	operationPos := 0
	txNumPos := operationPos + utils.IntSize
	txNum := page.GetInt(txNumPos)

	fileNamePos := txNumPos + utils.IntSize
	fileName, err := page.GetString(fileNamePos)
	if err != nil {
		return nil, err
	}

	blockNumPos := fileNamePos + file.MaxLength(len(fileName))
	blockNum := page.GetInt(blockNumPos)
	block := &file.BlockId{File: fileName, BlockNumber: int(blockNum)}

	offsetPos := blockNumPos + utils.IntSize
	offset := page.GetInt(offsetPos)

	valuePos := offsetPos + utils.IntSize
	value, err := page.GetString(valuePos)
	if err != nil {
		return nil, err
	}

	return &SetStringRecord{txNum: txNum, offset: offset, value: value, block: block}, nil
}

// Op returns the type of the log record.
func (r *SetStringRecord) Op() LogRecordType {
	return SetString
}

// TxNumber returns the transaction number stored in the log record.
func (r *SetStringRecord) TxNumber() int {
	return r.txNum
}

// String returns a string representation of the log record.
func (r *SetStringRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %s %d %s>", r.txNum, r.block, r.offset, r.value)
}

// Undo replaces the specified data value with the value saved in the log record.
// The method pins a buffer to the specified block,
// calls the buffer's setString method to restore the saved value, and unpins the buffer.
func (r *SetStringRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}
	defer tx.Unpin(r.block)
	return tx.SetString(r.block, r.offset, r.value, false) // Don't log the undo
}

// WriteSetStringToLog writes a set string record to the log. The record contains the specified transaction number, the
// filename and block number of the block containing the string, the offset of the string in the block, and the new value
// of the string.
// The method returns the LSN of the new log record.
func WriteSetStringToLog(logManager *log.Manager, txNum int, block *file.BlockId, offset int, value string) (int, error) {
	operationPos := 0
	txNumPos := operationPos + utils.IntSize
	fileNamePos := txNumPos + utils.IntSize
	fileName := block.Filename()

	blockNumPos := fileNamePos + file.MaxLength(len(fileName))
	blockNum := block.Number()

	offsetPos := blockNumPos + utils.IntSize
	valuePos := offsetPos + utils.IntSize
	recordLen := valuePos + file.MaxLength(len(value))

	recordBytes := make([]byte, recordLen)
	page := file.NewPageFromBytes(recordBytes)

	page.SetInt(operationPos, int(SetString))
	page.SetInt(txNumPos, txNum)
	if err := page.SetString(fileNamePos, fileName); err != nil {
		return -1, err
	}
	page.SetInt(blockNumPos, blockNum)
	page.SetInt(offsetPos, offset)
	if err := page.SetString(valuePos, value); err != nil {
		return -1, err
	}

	return logManager.Append(recordBytes)
}
