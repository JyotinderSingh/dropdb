package tx

import (
	"errors"
	"github.com/JyotinderSingh/dropdb/file"
)

// LogRecordType is the type of log record.
type LogRecordType int

const (
	Checkpoint LogRecordType = iota
	Start
	Commit
	Rollback
	SetInt
	SetString
)

func (t LogRecordType) String() string {
	switch t {
	case Checkpoint:
		return "Checkpoint"
	case Start:
		return "Start"
	case Commit:
		return "Commit"
	case Rollback:
		return "Rollback"
	case SetInt:
		return "SetInt"
	case SetString:
		return "SetString"
	default:
		return "Unknown"
	}
}

func FromCode(code int) (LogRecordType, error) {
	switch code {
	case 0:
		return Checkpoint, nil
	case 1:
		return Start, nil
	case 2:
		return Commit, nil
	case 3:
		return Rollback, nil
	case 4:
		return SetInt, nil
	case 5:
		return SetString, nil
	default:
		return -1, errors.New("unknown LogRecordType code")
	}
}

// LogRecord interface for log records.
type LogRecord interface {
	// Op returns the log record type.
	Op() LogRecordType

	// TxNumber returns the transaction ID stored with the log record.
	TxNumber() int

	// Undo undoes the operation encoded by this log record.
	// Undoes the operation encoded by this log record.
	// The only log record types for which this method does anything interesting are SETINT and SETSTRING.
	Undo(tx *Transaction) error
}

// CreateLogRecord interprets the bytes to create the appropriate log record. This method assumes that the first 4 bytes
// of the byte array represent the log record type.
func CreateLogRecord(bytes []byte) (LogRecord, error) {
	p := file.NewPageFromBytes(bytes)
	code := p.GetInt(0)
	recordType, err := FromCode(int(code))
	if err != nil {
		return nil, err
	}

	switch recordType {
	case Checkpoint:
		return NewCheckpointRecord()
	case Start:
		return NewStartRecord(p)
	case Commit:
		return NewCommitRecord(p)
	case Rollback:
		return NewRollbackRecord(p)
	case SetInt:
		return NewSetIntRecord(p)
	case SetString:
		return NewSetStringRecord(p)
	default:
		return nil, errors.New("unexpected LogRecordType")
	}
}
