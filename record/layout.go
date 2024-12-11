package record

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/utils"
	"sort"
)

// Layout describes the structure of a record.
// It contains the name, type, length, and offset of
// each field of a given table.
type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

// NewLayout creates a new layout for a given schema.
// The layout introduces padding between fields to ensure that each field is aligned
// correctly to their respective alignment requirements. Certain types require specific
// alignment sizes (e.g., longs are 8 bytes aligned).
// The layout is optimized for space efficiency by placing fields with larger alignment
// requirements first, which minimizes padding between fields.
func NewLayout(schema *Schema) *Layout {
	layout := &Layout{
		schema:  schema,
		offsets: make(map[string]int),
	}

	// Determine the alignment and sizes of fields
	fieldAlignments := make(map[string]int)
	for _, field := range schema.Fields() {
		fieldAlignments[field] = alignmentRequirement(schema.Type(field))
	}

	// Sort fields by their alignment requirements in descending order.
	// This ensures that fields with larger alignment requirements are placed first, which
	// minimizes padding between fields and reduces the overall size of the record.
	fields := schema.Fields()
	sort.Slice(fields, func(i, j int) bool {
		return fieldAlignments[fields[i]] > fieldAlignments[fields[j]]
	})

	pos := utils.IntSize // Reserve space for the empty/in-use field.
	for _, field := range fields {
		align := fieldAlignments[field]

		// Ensure alignment for the current field
		if pos%align != 0 {
			pos += align - (pos % align)
		}

		// Set the offset for the field
		layout.offsets[field] = pos

		// Move the position by the field's size
		pos += layout.lengthInBytes(field)
	}

	// Align the total slot size to the largest alignment requirement
	largestAlignment := maxAlignment(fieldAlignments)
	if pos%largestAlignment != 0 {
		pos += largestAlignment - (pos % largestAlignment)
	}

	layout.slotSize = pos
	return layout
}

// NewLayoutFromMetadata creates a new layout from the specified metadata.
// This method is used when the metadata is retrieved from the catalog.
func NewLayoutFromMetadata(schema *Schema, offsets map[string]int, slotSize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

// Schema returns the schema of the table's records.
func (l *Layout) Schema() *Schema {
	return l.schema
}

// Offset returns the offset of the specified field within a record based on the layout.
func (l *Layout) Offset(fieldName string) (int, bool) {
	offset, ok := l.offsets[fieldName]
	return offset, ok
}

// SlotSize returns the size of a record slot in bytes.
func (l *Layout) SlotSize() int {
	return l.slotSize
}

// lengthInBytes returns the length of a field in bytes.
func (l *Layout) lengthInBytes(fieldName string) int {
	fieldType := l.schema.Type(fieldName)

	switch fieldType {
	case Integer:
		return utils.IntSize
	case Long:
		return 8 // 8 bytes for long
	case Short:
		return 2 // 2 bytes for short
	case Boolean:
		return 1 // 1 byte for boolean
	case Date:
		return 8 // 8 bytes for date (64 bit Unix timestamp)
	case Varchar:
		return file.MaxLength(l.schema.Length(fieldName))
	default:
		panic(fmt.Sprintf("Unknown field type: %d", fieldType))
	}
}
