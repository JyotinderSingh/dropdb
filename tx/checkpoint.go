package tx

import (
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
)

type CheckpointRecord struct {
	LogRecord
}

// NewCheckpointRecord creates a new CheckpointRecord from a Page.
func NewCheckpointRecord() (*CheckpointRecord, error) {
	return &CheckpointRecord{}, nil
}

// Op returns the type of the log record.
func (r *CheckpointRecord) Op() LogRecordType {
	return Checkpoint
}

// TxNumber returns the transaction number stored in the log record. CheckpointRecord does not have a transaction
// number, so it returns a "dummy", negative txId.
func (r *CheckpointRecord) TxNumber() int {
	return -1
}

// Undo does nothing. CheckpointRecord does not change any data.
func (r *CheckpointRecord) Undo(_ *Transaction) error {
	return nil
}

// String returns a string representation of the log record.
func (r *CheckpointRecord) String() string {
	return "<CHECKPOINT>"
}

// WriteCheckpointToLog writes a checkpoint record to the log. This log record contains the Checkpoint operator and
// nothing else.
// The method returns the LSN of the new log record.
func WriteCheckpointToLog(logManager *log.Manager) (int, error) {
	record := make([]byte, 4)

	page := file.NewPageFromBytes(record)
	page.SetInt(0, int(Checkpoint))

	return logManager.Append(record)
}
