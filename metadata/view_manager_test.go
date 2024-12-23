package metadata

import (
	"testing"

	"github.com/JyotinderSingh/dropdb/tablescan"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestViewManager(t *testing.T) (*ViewManager, *tx.Transaction, func()) {
	tm, txn, cleanup := setupTestMetadata(800, t) // Assume setupTestMetadata initializes a TableManager and Transaction
	viewManager, err := NewViewManager(true, tm, txn)
	require.NoError(t, err)
	return viewManager, txn, cleanup
}

func TestViewManager_CreateView(t *testing.T) {
	vm, txn, cleanup := setupTestViewManager(t)
	defer cleanup()

	// Create a view
	viewName := "test_view"
	viewDefinition := "SELECT * FROM test_table"
	err := vm.CreateView(viewName, viewDefinition, txn)
	require.NoError(t, err)

	// Validate the view exists in the view catalog
	layout, err := vm.tableManager.GetLayout(viewCatalogTableName, txn)
	require.NoError(t, err)

	viewCatalogScan, err := tablescan.NewTableScan(txn, viewCatalogTableName, layout)
	require.NoError(t, err)
	defer viewCatalogScan.Close()

	err = viewCatalogScan.BeforeFirst()
	require.NoError(t, err)

	found := false
	for {
		hasNext, err := viewCatalogScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		name, err := viewCatalogScan.GetString(viewNameField)
		require.NoError(t, err)

		if name == viewName {
			definition, err := viewCatalogScan.GetString(viewDefinitionField)
			require.NoError(t, err)
			assert.Equal(t, viewDefinition, definition, "View definition mismatch")
			found = true
			break
		}
	}

	assert.True(t, found, "View not found in view catalog")
}

func TestViewManager_GetViewDefinition(t *testing.T) {
	vm, txn, cleanup := setupTestViewManager(t)
	defer cleanup()

	// Create a view
	viewName := "test_view"
	viewDefinition := "SELECT * FROM test_table"
	err := vm.CreateView(viewName, viewDefinition, txn)
	require.NoError(t, err)

	// Retrieve the view definition
	retrievedDefinition, err := vm.GetViewDefinition(viewName, txn)
	require.NoError(t, err)
	assert.Equal(t, viewDefinition, retrievedDefinition, "Retrieved view definition mismatch")
}
