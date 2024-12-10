package tx

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/utils"
)

type SetLongRecord struct {
	LogRecord
	txNum  int
	offset int
	value  int64
	block  *file.BlockId
}

func NewSetLongRecord(page *file.Page) (*SetLongRecord, error) {
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
	val := page.GetLong(valuePos) // 8 bytes long

	return &SetLongRecord{txNum: txNum, offset: offset, value: val, block: block}, nil
}

func (r *SetLongRecord) Op() LogRecordType {
	return SetLong
}

func (r *SetLongRecord) TxNumber() int {
	return r.txNum
}

func (r *SetLongRecord) String() string {
	return fmt.Sprintf("<SETLONG %d %s %d %d>", r.txNum, r.block, r.offset, r.value)
}

func (r *SetLongRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}
	defer tx.Unpin(r.block)
	return tx.SetLong(r.block, r.offset, r.value, false)
}

func WriteSetLongToLog(logManager *log.Manager, txNum int, block *file.BlockId, offset int, val int64) (int, error) {
	operationPos := 0
	txNumPos := operationPos + utils.IntSize
	fileNamePos := txNumPos + utils.IntSize
	fileName := block.Filename()

	blockNumPos := fileNamePos + file.MaxLength(len(fileName))
	blockNum := block.Number()

	offsetPos := blockNumPos + utils.IntSize
	valuePos := offsetPos + utils.IntSize
	// int64 is 8 bytes
	recordLen := valuePos + 8

	recordBytes := make([]byte, recordLen)
	page := file.NewPageFromBytes(recordBytes)

	page.SetInt(operationPos, int(SetLong))
	page.SetInt(txNumPos, txNum)
	if err := page.SetString(fileNamePos, fileName); err != nil {
		return -1, err
	}
	page.SetInt(blockNumPos, blockNum)
	page.SetInt(offsetPos, offset)
	page.SetLong(valuePos, val)

	return logManager.Append(recordBytes)
}
