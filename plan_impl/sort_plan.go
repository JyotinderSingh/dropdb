package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/materialize"
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/tx"
)

var _ plan.Plan = (*SortPlan)(nil)

// SortPlan implements the sort operator
type SortPlan struct {
	transaction *tx.Transaction
	inputPlan   plan.Plan
	schema      *record.Schema
	comparator  *query.RecordComparator
}

// NewSortPlan creates a new sort plan for the specified query
func NewSortPlan(transaction *tx.Transaction, p plan.Plan, sortFields []string) *SortPlan {
	return &SortPlan{
		transaction: transaction,
		inputPlan:   p,
		schema:      p.Schema(),
		comparator:  query.NewRecordComparator(sortFields),
	}
}

// Open is where most of the action is.
// Up to two sorted temporary tables are created,
// and are passed into SortScan for final merging.
func (sp *SortPlan) Open() (scan.Scan, error) {
	// Open the source scan
	src, err := sp.inputPlan.Open()
	if err != nil {
		return nil, err
	}
	defer src.Close()

	// Split into sorted runs
	runs, err := sp.splitIntoRuns(src)
	if err != nil {
		return nil, err
	}

	// Repeatedly merge runs until at most 2 remain
	for len(runs) > 2 {
		runs, err = sp.doAMergeIteration(runs)
		if err != nil {
			return nil, err
		}
	}

	// Create sort scan with final run(s)
	return query.NewSortScan(runs, sp.comparator)
}

// BlocksAccessed returns the number of blocks in the sorted table,
// which is the same as it would be in a materialized table.
// It does not include the one-time cost of materializing and sorting the records.
func (sp *SortPlan) BlocksAccessed() int {
	// Does not include the one-time cost of sorting
	mp := NewMaterializePlan(sp.transaction, sp.inputPlan)
	return mp.BlocksAccessed()
}

// RecordsOutput returns the number of records in the sorted table,
// which is the same as in the underlying query.
func (sp *SortPlan) RecordsOutput() int {
	return sp.inputPlan.RecordsOutput()
}

// DistinctValues returns the number of distinct field values in the sorted table,
// which is the same as in the underlying query.
func (sp *SortPlan) DistinctValues(fieldName string) int {
	return sp.inputPlan.DistinctValues(fieldName)
}

// Schema returns the schema of the sorted table,
// which is the same as in the underlying query.
func (sp *SortPlan) Schema() *record.Schema {
	return sp.schema
}

// splitIntoRuns splits the records from the source scan into sorted runs
func (sp *SortPlan) splitIntoRuns(src scan.Scan) ([]*materialize.TempTable, error) {
	var temps = make([]*materialize.TempTable, 0)

	if err := src.BeforeFirst(); err != nil {
		return nil, err
	}

	hasNext, err := src.Next()
	if err != nil || !hasNext {
		return temps, err
	}

	currentTemp := materialize.NewTempTable(sp.transaction, sp.schema)
	temps = append(temps, currentTemp)

	currentScan, err := currentTemp.Open()
	if err != nil {
		return nil, err
	}
	defer currentScan.Close()

	for {
		if err := sp.copy(src, currentScan); err != nil {
			return nil, err
		}

		hasNext, err = src.Next()
		if err != nil || !hasNext {
			break
		}

		if sp.comparator.Compare(src, currentScan) < 0 {
			// Start a new run
			currentScan.Close()
			currentTemp = materialize.NewTempTable(sp.transaction, sp.schema)
			temps = append(temps, currentTemp)

			currentScan, err = currentTemp.Open()
			if err != nil {
				return nil, err
			}
		}
	}

	return temps, nil
}

// doAMergeIteration merges pairs of runs until at most one run is left
func (sp *SortPlan) doAMergeIteration(runs []*materialize.TempTable) ([]*materialize.TempTable, error) {
	var result []*materialize.TempTable

	for len(runs) > 1 {
		p1 := runs[0]
		p2 := runs[1]
		runs = runs[2:]

		merged, err := sp.mergeTwoRuns(p1, p2)
		if err != nil {
			return nil, err
		}
		result = append(result, merged)
	}

	// Add any remaining run
	if len(runs) == 1 {
		result = append(result, runs[0])
	}

	return result, nil
}

// mergeTwoRuns merges two sorted runs into a single sorted run
func (sp *SortPlan) mergeTwoRuns(p1, p2 *materialize.TempTable) (*materialize.TempTable, error) {
	src1, err := p1.Open()
	if err != nil {
		return nil, err
	}
	defer src1.Close()

	src2, err := p2.Open()
	if err != nil {
		return nil, err
	}
	defer src2.Close()

	result := materialize.NewTempTable(sp.transaction, sp.schema)
	dest, err := result.Open()
	if err != nil {
		return nil, err
	}
	defer dest.Close()

	hasMore1, err := src1.Next()
	if err != nil {
		return nil, err
	}

	hasMore2, err := src2.Next()
	if err != nil {
		return nil, err
	}

	// Merge while both runs have records
	for hasMore1 && hasMore2 {
		if sp.comparator.Compare(src1, src2) < 0 {
			if err := sp.copy(src1, dest); err != nil {
				return nil, err
			}
			hasMore1, err = src1.Next()
		} else {
			if err := sp.copy(src2, dest); err != nil {
				return nil, err
			}
			hasMore2, err = src2.Next()
		}
		if err != nil {
			return nil, err
		}
	}

	// Copy remaining records from first run
	for hasMore1 {
		if err := sp.copy(src1, dest); err != nil {
			return nil, err
		}
		hasMore1, err = src1.Next()
		if err != nil {
			return nil, err
		}
	}

	// Copy remaining records from second run
	for hasMore2 {
		if err := sp.copy(src2, dest); err != nil {
			return nil, err
		}
		hasMore2, err = src2.Next()
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// copy copies a record from src to dest
func (sp *SortPlan) copy(src scan.Scan, dest scan.UpdateScan) error {
	if err := dest.Insert(); err != nil {
		return err
	}

	for _, fldName := range sp.schema.Fields() {
		val, err := src.GetVal(fldName)
		if err != nil {
			return err
		}
		if err := dest.SetVal(fldName, val); err != nil {
			return err
		}
	}

	return nil
}
