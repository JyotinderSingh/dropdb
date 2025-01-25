package hash

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/index"
	"github.com/JyotinderSingh/dropdb/index/common"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/table"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/utils"
)

const (
	numBuckets = 100
)

// ensure index interface is implemented
var _ index.Index = (*Index)(nil)

type Index struct {
	transaction *tx.Transaction
	indexName   string
	layout      *record.Layout
	searchKey   any
	tableScan   *table.Scan
}

// NewIndex opens a hash index for the specified index.
func NewIndex(transaction *tx.Transaction, indexName string, layout *record.Layout) index.Index {
	return &Index{
		transaction: transaction,
		indexName:   indexName,
		layout:      layout,
		searchKey:   nil,
		tableScan:   nil,
	}
}

// BeforeFirst positions the index before the first index record having the specified search key.
// The method hashes the search key to determine the bucket,
// and then opens a table scan on the file corresponding to that bucket.
// The table scan for the previous bucket (if any) is closed.
func (idx *Index) BeforeFirst(searchKey any) error {
	idx.Close()
	idx.searchKey = searchKey
	hashValue, err := utils.HashValue(searchKey)
	if err != nil {
		return err
	}
	bucket := hashValue % numBuckets
	tableName := fmt.Sprintf("%s-%d", idx.indexName, bucket)
	idx.tableScan, err = table.NewTableScan(idx.transaction, tableName, idx.layout)
	return err
}

// Next moves to the next index record having the search key.
// The method loops through the table scan for the bucket, looking for a matching record,
// and returns false if there are no more such records.
func (idx *Index) Next() (bool, error) {
	for {
		hasNext, err := idx.tableScan.Next()
		if err != nil || !hasNext {
			return false, err
		}

		currentValue, err := idx.tableScan.GetVal(common.DataValueField)
		if err != nil {
			return false, err
		}
		if currentValue == idx.searchKey {
			return true, nil
		}
	}
}

// GetDataRecordID retrieves the data record ID from the current record in the table scan for the bucket.
func (idx *Index) GetDataRecordID() (*record.ID, error) {
	blockNumber, err := idx.tableScan.GetInt(common.BlockField)
	if err != nil {
		return nil, err
	}
	id, err := idx.tableScan.GetInt(common.IDField)
	if err != nil {
		return nil, err
	}

	return record.NewID(blockNumber, id), nil
}

// Insert inserts a new record into the table scan for the bucket.
func (idx *Index) Insert(dataValue any, dataRecordID *record.ID) error {
	if err := idx.BeforeFirst(dataValue); err != nil {
		return err
	}

	if err := idx.tableScan.Insert(); err != nil {
		return err
	}
	if err := idx.tableScan.SetInt(common.BlockField, dataRecordID.BlockNumber()); err != nil {
		return err
	}
	if err := idx.tableScan.SetInt(common.IDField, dataRecordID.Slot()); err != nil {
		return err
	}
	return idx.tableScan.SetVal(common.DataValueField, dataValue)
}

// Delete deletes the specified record from the table scan for the bucket.
// The method starts at the beginning of the scan, and loops through the
// records until the specified record is found. If the record is found, it is deleted.
// If the record is not found, the method does nothing and does not return an error.
func (idx *Index) Delete(dataValue any, dataRecordID *record.ID) error {
	if err := idx.BeforeFirst(dataValue); err != nil {
		return err
	}

	for {
		hasNext, err := idx.tableScan.Next()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		currentRecordID, err := idx.GetDataRecordID()
		if err != nil {
			return err
		}

		if currentRecordID.Equals(dataRecordID) {
			return idx.tableScan.Delete()
		}
	}

	return nil
}

// Close closes the index by closing the current table scan.
func (idx *Index) Close() {
	if idx.tableScan != nil {
		idx.tableScan.Close()
		idx.tableScan = nil
	}
}

// SearchCost returns the cost of searching an index file having
// the specified number of blocks.
// the method assumes that all buckets are about the same size,
// so the cost is simply the size of the bucket.
func SearchCost(numBlocks, recordsPerBucket int) int {
	return numBlocks / numBuckets
}
