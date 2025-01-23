package tx

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/types"
	"time"

	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
)

type SetDateRecord struct {
	LogRecord
	txNum  int
	offset int
	value  time.Time
	block  *file.BlockId
}

func NewSetDateRecord(page *file.Page) (*SetDateRecord, error) {
	operationPos := 0
	txNumPos := operationPos + types.IntSize
	txNum := page.GetInt(txNumPos)

	fileNamePos := txNumPos + types.IntSize
	fileName, err := page.GetString(fileNamePos)
	if err != nil {
		return nil, err
	}

	blockNumPos := fileNamePos + file.MaxLength(len(fileName))
	blockNum := page.GetInt(blockNumPos)
	block := &file.BlockId{File: fileName, BlockNumber: int(blockNum)}

	offsetPos := blockNumPos + types.IntSize
	offset := page.GetInt(offsetPos)

	valuePos := offsetPos + types.IntSize
	val := page.GetDate(valuePos)

	return &SetDateRecord{txNum: txNum, offset: offset, value: val, block: block}, nil
}

func (r *SetDateRecord) Op() LogRecordType {
	return SetDate
}

func (r *SetDateRecord) TxNumber() int {
	return r.txNum
}

func (r *SetDateRecord) String() string {
	return fmt.Sprintf("<SETDATE %d %s %d %s>", r.txNum, r.block, r.offset, r.value.String())
}

func (r *SetDateRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}
	defer tx.Unpin(r.block)
	return tx.SetDate(r.block, r.offset, r.value, false)
}

func WriteSetDateToLog(logManager *log.Manager, txNum int, block *file.BlockId, offset int, val time.Time) (int, error) {
	operationPos := 0
	txNumPos := operationPos + types.IntSize
	fileNamePos := txNumPos + types.IntSize
	fileName := block.Filename()

	blockNumPos := fileNamePos + file.MaxLength(len(fileName))
	blockNum := block.Number()

	offsetPos := blockNumPos + types.IntSize
	valuePos := offsetPos + types.IntSize
	// time.Time stored as int64 (8 bytes)
	recordLen := valuePos + 8

	recordBytes := make([]byte, recordLen)
	page := file.NewPageFromBytes(recordBytes)

	page.SetInt(operationPos, int(SetDate))
	page.SetInt(txNumPos, txNum)
	if err := page.SetString(fileNamePos, fileName); err != nil {
		return -1, err
	}
	page.SetInt(blockNumPos, blockNum)
	page.SetInt(offsetPos, offset)
	page.SetDate(valuePos, val)

	return logManager.Append(recordBytes)
}
