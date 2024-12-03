package tx

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/utils"
)

type SetIntRecord struct {
	LogRecord
	txNum  int
	offset int
	value  int
	block  *file.BlockId
}

// NewSetIntRecord creates a new SetIntRecord from a Page.
func NewSetIntRecord(page *file.Page) (*SetIntRecord, error) {
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
	value := page.GetInt(valuePos)

	return &SetIntRecord{txNum: txNum, offset: offset, value: value, block: block}, nil
}

// Op returns the type of the log record.
func (r *SetIntRecord) Op() LogRecordType {
	return SetInt
}

// TxNumber returns the transaction number stored in the log record.
func (r *SetIntRecord) TxNumber() int {
	return r.txNum
}

// String returns a string representation of the log record.
func (r *SetIntRecord) String() string {
	return fmt.Sprintf("<SETINT %d %s %d %d>", r.txNum, r.block, r.offset, r.value)
}

// Undo replaces the specified data value with the value saved in the log record.
// The method pins a buffer to the specified block,
// calls setInt to restore the saved value,
// and unpins the buffer.
func (r *SetIntRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}
	defer tx.Unpin(r.block)
	return tx.SetInt(r.block, r.offset, r.value, false)
}

// WriteSetIntToLog writes a SetInt record to the log. The record contains the specified transaction number, the
// filename and block number of the block containing the int, the offset of the int in the block, and the new value
// of the int.
// The method returns the LSN of the new log record.
func WriteSetIntToLog(logManager *log.Manager, txNum int, block *file.BlockId, offset, val int) (int, error) {
	operationPos := 0
	txNumPos := operationPos + utils.IntSize
	fileNamePos := txNumPos + utils.IntSize
	fileName := block.Filename()

	blockNumPos := fileNamePos + file.MaxLength(len(block.File))
	blockNum := block.Number()

	offsetPos := blockNumPos + utils.IntSize
	valuePos := offsetPos + utils.IntSize
	recordLen := valuePos + utils.IntSize

	recordBytes := make([]byte, recordLen)
	page := file.NewPageFromBytes(recordBytes)

	page.SetInt(operationPos, int(SetInt))
	page.SetInt(txNumPos, txNum)
	if err := page.SetString(fileNamePos, fileName); err != nil {
		return -1, err
	}
	page.SetInt(blockNumPos, blockNum)
	page.SetInt(offsetPos, offset)
	page.SetInt(valuePos, val)

	return logManager.Append(recordBytes)
}
