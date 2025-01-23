package tx

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/types"
)

type CommitRecord struct {
	LogRecord
	txNum int
}

// NewCommitRecord creates a new CommitRecord from a Page.
func NewCommitRecord(page *file.Page) (*CommitRecord, error) {
	operationPos := 0
	txNumPos := operationPos + types.IntSize
	txNum := page.GetInt(txNumPos)

	return &CommitRecord{txNum: txNum}, nil
}

// Op returns the type of the log record.
func (r *CommitRecord) Op() LogRecordType {
	return Commit
}

// TxNumber returns the transaction number stored in the log record.
func (r *CommitRecord) TxNumber() int {
	return r.txNum
}

// Undo does nothing. CommitRecord does not change any data.
func (r *CommitRecord) Undo(_ *Transaction) error {
	return nil
}

// String returns a string representation of the log record.
func (r *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", r.txNum)
}

// WriteCommitToLog writes a commit record to the log. This log record contains the Commit operator,
// followed by the transaction id.
// The method returns the LSN of the new log record.
func WriteCommitToLog(logManager *log.Manager, txNum int) (int, error) {
	record := make([]byte, 2*types.IntSize)

	page := file.NewPageFromBytes(record)
	page.SetInt(0, int(Commit))
	page.SetInt(types.IntSize, txNum)

	return logManager.Append(record)
}
