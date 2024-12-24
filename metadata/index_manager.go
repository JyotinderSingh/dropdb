package metadata

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tablescan"
	"github.com/JyotinderSingh/dropdb/tx"
)

const (
	indexCatalogTable = "index_catalog"
	indexNameField    = "index_name"
)

// IndexManager is responsible for managing indexes in the database.
type IndexManager struct {
	layout       *record.Layout
	tableManager *TableManager
	StatManager  *StatManager
}

// NewIndexManager creates a new IndexManager instance.
// This method is called during system startup.
// If the database is new, then the idxCatalog is created.
func NewIndexManager(isNew bool, tableManager *TableManager, statManager *StatManager, transaction *tx.Transaction) (*IndexManager, error) {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField(indexNameField, maxNameLength)
		schema.AddStringField(tableNameField, maxNameLength)
		schema.AddStringField(fieldNameField, maxNameLength)

		if err := tableManager.CreateTable(indexCatalogTable, schema, transaction); err != nil {
			return nil, err
		}
	}

	layout, err := tableManager.GetLayout(indexCatalogTable, transaction)
	if err != nil {
		return nil, err
	}

	return &IndexManager{
		layout:       layout,
		tableManager: tableManager,
		StatManager:  statManager,
	}, nil
}

// CreateIndex creates a new index of the specified type for the specified field.
// A unique ID is assigned to this index, and its information is stored in the indexCatalogTable.
func (im *IndexManager) CreateIndex(indexName, tableName, fieldName string, transaction *tx.Transaction) error {
	tableScan, err := tablescan.NewTableScan(transaction, indexCatalogTable, im.layout)
	if err != nil {
		return fmt.Errorf("failed to create table scan: %w", err)
	}
	defer tableScan.Close()

	if err := tableScan.Insert(); err != nil {
		return fmt.Errorf("failed to insert into table scan: %w", err)
	}

	if err := tableScan.SetString(indexNameField, indexName); err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}

	if err := tableScan.SetString(tableNameField, tableName); err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}

	if err := tableScan.SetString(fieldNameField, fieldName); err != nil {
		return fmt.Errorf("failed to set string: %w", err)
	}

	return nil
}

// GetIndexInfo returns a map containing the index info for all indexes on the specified table.
func (im *IndexManager) GetIndexInfo(tableName string, transaction *tx.Transaction) (map[string]*IndexInfo, error) {
	tableScan, err := tablescan.NewTableScan(transaction, indexCatalogTable, im.layout)
	if err != nil {
		return nil, err
	}
	defer tableScan.Close()

	result := make(map[string]*IndexInfo)

	for {
		hasNext, err := tableScan.Next()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		currentTableName, err := tableScan.GetString(tableNameField)
		if err != nil {
			return nil, err
		}
		if currentTableName != tableName {
			continue
		}

		var indexName, fieldName string

		if indexName, err = tableScan.GetString(indexNameField); err != nil {
			return nil, err
		}
		if fieldName, err = tableScan.GetString(fieldNameField); err != nil {
			return nil, err
		}

		tableLayout, err := im.tableManager.GetLayout(tableName, transaction)
		if err != nil {
			return nil, err
		}

		tableStatInfo, err := im.StatManager.GetStatInfo(tableName, tableLayout, transaction)
		if err != nil {
			return nil, err
		}

		indexInfo := NewIndexInfo(indexName, fieldName, tableLayout.Schema(), transaction, tableStatInfo)
		result[fieldName] = indexInfo
	}

	return result, nil
}
