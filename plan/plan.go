package plan

import (
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
)

type Plan interface {
	// Open opens a scan corresponding to this plan.
	// The scan will be positioned before its first record.
	Open() (scan.Scan, error)

	// BlocksAccessed returns the estimated number of
	// block accesses that will occur when the scan is read to completion.
	BlocksAccessed() int

	// RecordsOutput returns the estimated number of records
	// in the query's output table.
	RecordsOutput() int

	// DistinctValues returns the estimated number of distinct values
	// for the specified field in the query's output table.
	DistinctValues(fieldName string) int

	// Schema returns the schema of the query's output table.
	Schema() *record.Schema
}
