package metadata

import (
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tablescan"
	"github.com/JyotinderSingh/dropdb/tx"
)

const (
	maxViewDefinitionLength = 100
	viewNameField           = "view_name"
	viewDefinitionField     = "view_definition"
	viewCatalogTableName    = "view_catalog"
)

type ViewManager struct {
	tableManager *TableManager
}

// NewViewManager creates a new ViewManager.
func NewViewManager(isNew bool, tableManager *TableManager, tx *tx.Transaction) (*ViewManager, error) {
	vm := &ViewManager{tableManager: tableManager}

	if isNew {
		schema := record.NewSchema()
		schema.AddStringField(viewNameField, maxNameLength)
		schema.AddStringField(viewDefinitionField, maxViewDefinitionLength)
		if err := vm.tableManager.CreateTable(viewCatalogTableName, schema, tx); err != nil {
			return nil, err
		}
	}

	return vm, nil
}

// CreateView creates a view.
func (vm *ViewManager) CreateView(viewName, viewDefinition string, tx *tx.Transaction) error {
	layout, err := vm.tableManager.GetLayout(viewCatalogTableName, tx)
	if err != nil {
		return err
	}

	viewCatalogTableScan, err := tablescan.NewTableScan(tx, viewCatalogTableName, layout)
	if err != nil {
		return err
	}

	if err := viewCatalogTableScan.Insert(); err != nil {
		return err
	}
	if err := viewCatalogTableScan.SetString(viewNameField, viewName); err != nil {
		return err
	}
	if err := viewCatalogTableScan.SetString(viewDefinitionField, viewDefinition); err != nil {
		return err
	}

	return viewCatalogTableScan.Close()
}

// GetViewDefinition returns the definition of the specified view.
func (vm *ViewManager) GetViewDefinition(viewName string, tx *tx.Transaction) (string, error) {
	var result string
	layout, err := vm.tableManager.GetLayout(viewCatalogTableName, tx)
	if err != nil {
		return "", err
	}

	viewCatalogTableScan, err := tablescan.NewTableScan(tx, viewCatalogTableName, layout)
	if err != nil {
		return "", err
	}

	for {
		hasNext, err := viewCatalogTableScan.Next()
		if err != nil {
			return "", err
		}
		if !hasNext {
			break
		}

		name, err := viewCatalogTableScan.GetString(viewNameField)
		if err != nil {
			return "", err
		}

		if name == viewName {
			definition, err := viewCatalogTableScan.GetString(viewDefinitionField)
			if err != nil {
				return "", err
			}

			result = definition
			break
		}
	}

	if err := viewCatalogTableScan.Close(); err != nil {
		return "", err
	}

	return result, nil
}
