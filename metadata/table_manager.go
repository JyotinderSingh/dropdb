package metadata

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tablescan"
	"github.com/JyotinderSingh/dropdb/tx"
)

const (
	maxNameLength  = 16
	tableNameField = "table_name"
	slotSizeField  = "slot_size"
	fieldNameField = "field_name"
	typeField      = "type"
	lengthField    = "length"
	offsetField    = "offset"

	tableCatalogTableName = "table_catalog"
	fieldCatalogTableName = "field_catalog"
)

// TableManager manages table data.
// It has methods to create a atable, save the metadata in the catalog,
// and obtain the metadata of a previously created table.
type TableManager struct {
	tableCatalogLayout *record.Layout
	fieldCatalogLayout *record.Layout
}

// NewTableManager creates a new TableManager.
// This creates a new catalog manager for the database system.
// If the database is new, the two catalog tables are created.
func NewTableManager(isNew bool, tx *tx.Transaction) (*TableManager, error) {
	tm := &TableManager{}

	tableCatalogSchema := record.NewSchema()
	tableCatalogSchema.AddStringField(tableNameField, maxNameLength)
	tableCatalogSchema.AddIntField(slotSizeField)
	tm.tableCatalogLayout = record.NewLayout(tableCatalogSchema)

	fieldCatalogSchema := record.NewSchema()
	fieldCatalogSchema.AddStringField(tableNameField, maxNameLength)
	fieldCatalogSchema.AddStringField(fieldNameField, maxNameLength)
	fieldCatalogSchema.AddIntField(typeField)
	fieldCatalogSchema.AddIntField(lengthField)
	fieldCatalogSchema.AddIntField(offsetField)
	tm.fieldCatalogLayout = record.NewLayout(fieldCatalogSchema)

	if isNew {
		if err := tm.CreateTable(tableCatalogTableName, tableCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("failed to create table catalog: %w", err)
		}
		if err := tm.CreateTable(fieldCatalogTableName, fieldCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("failed to create field catalog: %w", err)
		}
	}

	return tm, nil
}

// CreateTable creates a new table having the specified name and schema.
func (tm *TableManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) error {
	layout := record.NewLayout(schema)

	// Insert the table into the table catalog
	if err := tm.insertIntoTableCatalog(tx, tableName, layout); err != nil {
		return fmt.Errorf("failed to insert into table catalog: %w", err)
	}

	// Insert the fields into the field catalog
	if err := tm.insertIntoFieldCatalog(tx, tableName, schema, layout); err != nil {
		return fmt.Errorf("failed to insert into field catalog: %w", err)
	}

	return nil
}

// insertIntoTableCatalog inserts a new record into the table catalog.
func (tm *TableManager) insertIntoTableCatalog(tx *tx.Transaction, tableName string, layout *record.Layout) error {
	tableCatalog, err := tablescan.NewTableScan(tx, tableCatalogTableName, tm.tableCatalogLayout)
	if err != nil {
		return err
	}

	if err := tableCatalog.Insert(); err != nil {
		return err
	}
	if err := tableCatalog.SetString(tableNameField, tableName); err != nil {
		return err
	}
	if err := tableCatalog.SetInt(slotSizeField, layout.SlotSize()); err != nil {
		return err
	}

	return tableCatalog.Close()
}

// insertIntoFieldCatalog inserts schema fields into the field catalog.
func (tm *TableManager) insertIntoFieldCatalog(tx *tx.Transaction, tableName string, schema *record.Schema, layout *record.Layout) error {
	fieldCatalog, err := tablescan.NewTableScan(tx, fieldCatalogTableName, tm.fieldCatalogLayout)
	if err != nil {
		return err
	}

	for _, field := range schema.Fields() {
		if err := fieldCatalog.Insert(); err != nil {
			return err
		}
		if err := fieldCatalog.SetString(tableNameField, tableName); err != nil {
			return err
		}
		if err := fieldCatalog.SetString(fieldNameField, field); err != nil {
			return err
		}
		if err := fieldCatalog.SetInt(typeField, int(schema.Type(field))); err != nil {
			return err
		}
		if err := fieldCatalog.SetInt(lengthField, schema.Length(field)); err != nil {
			return err
		}
		if err := fieldCatalog.SetInt(offsetField, layout.Offset(field)); err != nil {
			return err
		}
	}

	return fieldCatalog.Close()
}

func (tm *TableManager) TableCatalogLayout() *record.Layout {
	return tm.tableCatalogLayout
}

func (tm *TableManager) FieldCatalogLayout() *record.Layout {
	return tm.fieldCatalogLayout
}
