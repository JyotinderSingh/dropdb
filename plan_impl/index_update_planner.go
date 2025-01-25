package plan_impl

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/index"
	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/parse"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/tx"
)

var _ UpdatePlanner = &IndexUpdatePlanner{}

// IndexUpdatePlanner is a modification of the BasicUpdatePlanner that
// uses indexes to speed up update and delete operations.
// It dispatches each update statement to the corresponding index planner.
type IndexUpdatePlanner struct {
	metadataManager *metadata.Manager
}

func NewIndexUpdatePlanner(metadataManager *metadata.Manager) UpdatePlanner {
	return &IndexUpdatePlanner{metadataManager: metadataManager}
}

func (up *IndexUpdatePlanner) ExecuteInsert(data *parse.InsertData, transaction *tx.Transaction) (int, error) {
	tableName := data.TableName()
	tablePlan, err := NewTablePlan(transaction, tableName, up.metadataManager)
	if err != nil {
		return 0, err
	}

	// first, insert the record.
	tableScan, err := tablePlan.Open()
	if err != nil {
		return 0, err
	}
	updateScan, ok := tableScan.(scan.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("table scan is not an update scan")
	}
	defer updateScan.Close()

	if err := updateScan.Insert(); err != nil {
		return 0, err
	}
	recordID := updateScan.GetRecordID()

	// then modify each field, inserting an index record if appropriate.
	indexes, err := up.metadataManager.GetIndexInfo(tableName, transaction)
	if err != nil {
		return 0, err
	}

	vals := data.Values()
	for i, field := range data.Fields() {
		val := vals[i]
		if err := updateScan.SetVal(field, val); err != nil {
			return 0, err
		}

		indexInfo, ok := indexes[field]
		if !ok {
			continue
		}

		idx := indexInfo.Open()
		if err := idx.Insert(val, recordID); err != nil {
			return 0, err
		}
		idx.Close()
	}

	return 1, nil
}

func (up *IndexUpdatePlanner) ExecuteDelete(data *parse.DeleteData, transaction *tx.Transaction) (int, error) {
	tableName := data.TableName()
	tablePlan, err := NewTablePlan(transaction, tableName, up.metadataManager)
	if err != nil {
		return 0, err
	}
	selectPlan := NewSelectPlan(tablePlan, data.Predicate())
	indexes, err := up.metadataManager.GetIndexInfo(tableName, transaction)
	if err != nil {
		return 0, err
	}

	selectScan, err := selectPlan.Open()
	if err != nil {
		return 0, err
	}
	updateScan, ok := selectScan.(scan.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("select scan is not an update scan")
	}
	defer updateScan.Close()

	count := 0
	for {
		hasNext, err := updateScan.Next()
		if err != nil || !hasNext {
			return count, err
		}

		// 1. delete the record's RecordID from each index.
		recordID := updateScan.GetRecordID()
		for fieldName, indexInfo := range indexes {
			val, err := updateScan.GetVal(fieldName)
			if err != nil {
				return count, err
			}
			idx := indexInfo.Open()
			if err := idx.Delete(val, recordID); err != nil {
				idx.Close()
				return count, err
			}
			idx.Close()
		}

		// 2. delete the record.
		if err := updateScan.Delete(); err != nil {
			return count, err
		}
		count++
	}
}

func (up *IndexUpdatePlanner) ExecuteModify(data *parse.ModifyData, transaction *tx.Transaction) (int, error) {
	tableName := data.TableName()
	fieldName := data.TargetField()

	tablePlan, err := NewTablePlan(transaction, tableName, up.metadataManager)
	if err != nil {
		return 0, err
	}
	selectPlan := NewSelectPlan(tablePlan, data.Predicate())

	indexes, err := up.metadataManager.GetIndexInfo(tableName, transaction)
	if err != nil {
		return 0, err
	}

	var idx index.Index = nil
	if indexInfo, ok := indexes[fieldName]; ok {
		idx = indexInfo.Open()
		defer idx.Close()
	}

	selectScan, err := selectPlan.Open()
	if err != nil {
		return 0, err
	}
	updateScan, ok := selectScan.(scan.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("select scan is not an update scan")
	}
	defer updateScan.Close()

	count := 0
	for {
		hasNext, err := updateScan.Next()
		if err != nil || !hasNext {
			return count, err
		}

		newValue, err := data.NewValue().Evaluate(updateScan)
		if err != nil {
			return count, err
		}

		oldValue, err := updateScan.GetVal(fieldName)
		if err != nil {
			return count, err
		}

		if err := updateScan.SetVal(fieldName, newValue); err != nil {
			return count, err
		}

		// 1. delete the old value from the index.
		if idx != nil {
			recordID := updateScan.GetRecordID()
			if err := idx.Delete(oldValue, recordID); err != nil {
				return count, err
			}
			if err := idx.Insert(newValue, recordID); err != nil {
				return count, err
			}
		}

		count++
	}
}

func (up *IndexUpdatePlanner) ExecuteCreateTable(data *parse.CreateTableData, transaction *tx.Transaction) (int, error) {
	err := up.metadataManager.CreateTable(data.TableName(), data.NewSchema(), transaction)
	return 0, err
}

func (up *IndexUpdatePlanner) ExecuteCreateView(data *parse.CreateViewData, transaction *tx.Transaction) (int, error) {
	err := up.metadataManager.CreateView(data.ViewName(), data.ViewDefinition(), transaction)
	return 0, err
}

func (up *IndexUpdatePlanner) ExecuteCreateIndex(data *parse.CreateIndexData, transaction *tx.Transaction) (int, error) {
	err := up.metadataManager.CreateIndex(data.IndexName(), data.TableName(), data.FieldName(), transaction)
	return 0, err
}
