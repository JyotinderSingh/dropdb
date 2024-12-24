package index

import "github.com/JyotinderSingh/dropdb/record"

type Index interface {
	// BeforeFirst positions the index before the
	// first record having the specified search key.
	BeforeFirst(searchKey any) error

	// Next moves the index to the next record having the search key specified in the BeforeFirst method.
	// Returns false if there are no more such index records.
	Next() (bool, error)

	// GetDataRecordID returns the data record ID stored in the current index record.
	GetDataRecordID() (*record.ID, error)

	// Insert inserts a new index record having the specified dataValue and dataRecordID values.
	Insert(dataValue any, dataRecordID *record.ID) error

	// Delete deletes the index record having the specified dataValue and dataRecordID values.
	Delete(dataValue any, dataRecordID *record.ID) error

	// Close closes the index.
	Close()
}
