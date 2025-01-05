package plan

import (
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
)

type Plan interface {
	// Open opens a scan corresponding to this plan.
	// The scan will be positioned before its first record.
	Open() *scan.Scan

	// BlocksAccessed returns an estimate of the number of
	// block accesses that will occur when the scan is read to completion.
	BlocksAccessed() int

	// RecordsOutput returns an estimate of the number of records
	// in the query's output table.
	RecordsOutput() int

	// DistinctValues returns an estimate of the number of distinct
	// for the specified field in the query's output table.
	DistinctValues(fieldName string) int

	// Schema returns the schema of the query's output table.
	Schema() *record.Schema
}
