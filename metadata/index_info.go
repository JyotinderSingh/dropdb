package metadata

import (
	"github.com/JyotinderSingh/dropdb/index"
	"github.com/JyotinderSingh/dropdb/index/hash"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
)

type IndexInfo struct {
	indexName   string
	fieldName   string
	transaction *tx.Transaction
	tableSchema *record.Schema
	indexLayout *record.Layout
	statInfo    *StatInfo
}

// NewIndexInfo creates an IndexInfo object for the specified index.
func NewIndexInfo(indexName, fieldName string, tableSchema *record.Schema,
	transaction *tx.Transaction, statInfo *StatInfo) *IndexInfo {
	ii := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		transaction: transaction,
		tableSchema: tableSchema,
		statInfo:    statInfo,
	}
	ii.indexLayout = ii.CreateIndexLayout()
	return ii
}

// Open opens the index described by this object.
func (ii *IndexInfo) Open() index.Index {
	return hash.NewIndex(ii.transaction, ii.indexName, ii.indexLayout)
	//return NewBtreeIndex(ii.transaction, ii.indexName, ii.indexLayout)
}

// BlocksAccessed estimates the number of block accesses required to
// find all the index records having a particular search key.
// The method uses the table's metadata to estimate the size of the
// index file and the number of index records per block.
// It then passes this information to the traversalCost method of the
// appropriate index type, which then provides the estimate.
func (ii *IndexInfo) BlocksAccessed() int {
	recordsPerBlock := ii.transaction.BlockSize() / ii.indexLayout.SlotSize()
	numBlocks := ii.statInfo.RecordsOutput() / recordsPerBlock
	return hash.SearchCost(numBlocks, recordsPerBlock)
	//return BtreeIndex.SearchCost(numBlocks, recordsPerBlock)
}

// RecordsOutput returns the estimated number of records having a search key.
// This value is the same as doing a select query; that is, it is the number of records in the table
// divided by the number of distinct values of the indexed field.
func (ii *IndexInfo) RecordsOutput() int {
	return ii.statInfo.RecordsOutput() / ii.statInfo.DistinctValues(ii.fieldName)
}

// DistinctValues returns the number of distinct values for the indexed field
// in the underlying table, or 1 for the indexed field.
func (ii *IndexInfo) DistinctValues(fieldName string) int {
	if ii.fieldName == fieldName {
		return 1
	}
	return ii.statInfo.DistinctValues(fieldName)
}

// CreateIndexLayout returns the layout of the index records.
// The schema consists of the dataRecordID (which is represented as two integers,
// the block number and the record ID) and the dataValue (which is the indexed field).
// Schema information about the indexed field is obtained from the table's schema.
func (ii *IndexInfo) CreateIndexLayout() *record.Layout {
	schema := record.NewSchema()
	schema.AddIntField(index.BlockField)
	schema.AddIntField(index.IDField)
	switch ii.tableSchema.Type(ii.fieldName) {
	case record.Integer:
		schema.AddIntField(index.DataValueField)
	case record.Varchar:
		schema.AddStringField(index.DataValueField, ii.tableSchema.Length(ii.fieldName))
	case record.Boolean:
		schema.AddBoolField(index.DataValueField)
	case record.Long:
		schema.AddLongField(index.DataValueField)
	case record.Short:
		schema.AddShortField(index.DataValueField)
	case record.Date:
		schema.AddDateField(index.DataValueField)
	}

	return record.NewLayout(schema)
}
