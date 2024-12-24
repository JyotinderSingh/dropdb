package metadata

import (
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
)

type Manager struct {
	tableManager *TableManager
	viewManager  *ViewManager
	statManager  *StatManager
	indexManager *IndexManager
}

func NewManager(isNew bool, transaction *tx.Transaction) *Manager {
	m := &Manager{}

	var err error
	if m.tableManager, err = NewTableManager(isNew, transaction); err != nil {
		return nil
	}
	if m.viewManager, err = NewViewManager(isNew, m.tableManager, transaction); err != nil {
		return nil
	}
	if m.statManager, err = NewStatManager(m.tableManager, transaction, 100); err != nil {
		return nil
	}
	if m.indexManager, err = NewIndexManager(isNew, m.tableManager, m.statManager, transaction); err != nil {
		return nil
	}

	return m
}

// CreateTable creates a new table having the specified name and schema.
func (m *Manager) CreateTable(tableName string, schema *record.Schema, transaction *tx.Transaction) error {
	return m.tableManager.CreateTable(tableName, schema, transaction)
}

// GetLayout returns the layout of the specified table from the catalog.
func (m *Manager) GetLayout(tableName string, transaction *tx.Transaction) (*record.Layout, error) {
	return m.tableManager.GetLayout(tableName, transaction)
}

// CreateView creates a view.
func (m *Manager) CreateView(viewName, viewDefinition string, transaction *tx.Transaction) error {
	return m.viewManager.CreateView(viewName, viewDefinition, transaction)
}

// GetViewDefinition returns the definition of the specified view.
func (m *Manager) GetViewDefinition(viewName string, transaction *tx.Transaction) (string, error) {
	return m.viewManager.GetViewDefinition(viewName, transaction)
}

// CreateIndex creates a new index of the specified type for the specified field.
// A unique ID is assigned to this index, and its information is stored in the indexCatalogTable.
func (m *Manager) CreateIndex(indexName, tableName, fieldName string, transaction *tx.Transaction) error {
	return m.indexManager.CreateIndex(indexName, tableName, fieldName, transaction)
}

// GetIndexInfo returns a map containing the index info for all indexes on the specified table.
func (m *Manager) GetIndexInfo(tableName string, transaction *tx.Transaction) (map[string]*IndexInfo, error) {
	return m.indexManager.GetIndexInfo(tableName, transaction)
}

// GetStatInfo returns statistical information about the specified table.
// It refreshes statistics periodically based on the refreshLimit.
func (m *Manager) GetStatInfo(tableName string, layout *record.Layout, transaction *tx.Transaction) (*StatInfo, error) {
	return m.statManager.GetStatInfo(tableName, layout, transaction)
}
