package record

import (
	"github.com/JyotinderSingh/dropdb/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewLayout(t *testing.T) {
	tests := []struct {
		name          string
		schemaBuilder func() *Schema
		expectedOrder []string
		expectedSize  int
		expectedAlign map[string]int
	}{
		{
			name: "simple types with different alignments",
			schemaBuilder: func() *Schema {
				s := NewSchema()
				s.AddBoolField("flag")     // 1-byte align
				s.AddLongField("bigNum")   // 8-byte align
				s.AddShortField("counter") // 2-byte align
				return s
			},
			expectedOrder: []string{"bigNum", "counter", "flag"},
			expectedSize:  24, // utils.IntSize(header) + 8(long) + 2(short) + 1(bool) + padding
			expectedAlign: map[string]int{
				"bigNum":  8,
				"counter": 2,
				"flag":    1,
			},
		},
		{
			name: "mixed types with varchar",
			schemaBuilder: func() *Schema {
				s := NewSchema()
				s.AddStringField("name", 10) // 1-byte align, size is IntSize + (10 * 4)
				s.AddDateField("timestamp")  // 8-byte align
				s.AddIntField("count")       // utils.IntSize align
				return s
			},
			expectedOrder: []string{"timestamp", "count", "name"},
			expectedSize:  72, // utils.IntSize(header) + 8(date) + utils.IntSize(int) + (utils.IntSize + 10*4)(varchar)
			expectedAlign: map[string]int{
				"timestamp": 8,
				"count":     utils.IntSize,
				"name":      1,
			},
		},
		{
			name: "all types test",
			schemaBuilder: func() *Schema {
				s := NewSchema()
				s.AddBoolField("active")
				s.AddDateField("created")
				s.AddIntField("count")
				s.AddLongField("id")
				s.AddShortField("type")
				s.AddStringField("name", 15) // IntSize + (15 * 4) bytes
				return s
			},
			expectedOrder: []string{"created", "count", "id", "type", "active", "name"},
			expectedSize:  104, // utils.IntSize(header) + 8(date) + 8(long) + utils.IntSize(int) + 2(short) + 1(bool) + (utils.IntSize + 15*4)(varchar)
			expectedAlign: map[string]int{
				"created": 8,
				"id":      8,
				"count":   utils.IntSize,
				"type":    2,
				"name":    1,
				"active":  1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := tt.schemaBuilder()
			layout := NewLayout(schema)

			// Verify field order through offsets
			var lastOffset int
			var fields []string
			for _, field := range schema.Fields() {
				offset := layout.Offset(field)
				if offset > lastOffset {
					fields = append(fields, field)
					lastOffset = offset
				}
			}
			assert.Equal(t, tt.expectedOrder, fields, "Field order mismatch")

			// Verify slot size
			assert.Equal(t, tt.expectedSize, layout.SlotSize(), "Slot size mismatch")

			// Verify alignments
			for field, expectedAlign := range tt.expectedAlign {
				offset := layout.Offset(field)
				assert.Equal(t, 0, offset%expectedAlign,
					"Field %s is not properly aligned. Offset: %d, Required alignment: %d",
					field, offset, expectedAlign)
			}
		})
	}
}

func TestNewLayoutFromMetadata(t *testing.T) {
	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	offsets := map[string]int{
		"id":   utils.IntSize,                 // After header
		"name": utils.IntSize + utils.IntSize, // After header and int
	}
	slotSize := utils.IntSize + utils.IntSize + (utils.IntSize + 20*4) // header + int + varchar(length + data) + padding

	layout := NewLayoutFromMetadata(schema, offsets, slotSize)

	assert.Equal(t, schema, layout.Schema(), "Schema mismatch")
	assert.Equal(t, slotSize, layout.SlotSize(), "Slot size mismatch")

	// Verify offsets
	for field, expectedOffset := range offsets {
		offset := layout.Offset(field)
		assert.Equal(t, expectedOffset, offset, "Offset mismatch for field %s", field)
	}
}

func TestLayoutPaddingOptimization(t *testing.T) {
	tests := []struct {
		name          string
		schemaBuilder func() *Schema
		expectedSize  int
	}{
		{
			name: "worst case ordering",
			schemaBuilder: func() *Schema {
				s := NewSchema()
				s.AddBoolField("b1") // 1 byte
				s.AddLongField("l1") // 8 bytes
				s.AddBoolField("b2") // 1 byte
				s.AddLongField("l2") // 8 bytes
				return s
			},
			expectedSize: 32, // utils.IntSize(header) + 8(long) + 8(long) + 1(bool) + 1(bool) + padding
		},
		{
			name: "mixed field sizes with varchar",
			schemaBuilder: func() *Schema {
				s := NewSchema()
				s.AddStringField("s1", 3) // utils.IntSize + (3 * 4) bytes
				s.AddIntField("i1")       // utils.IntSize bytes
				s.AddBoolField("b1")      // 1 byte
				s.AddLongField("l1")      // 8 bytes
				return s
			},
			expectedSize: 48, // utils.IntSize(header) + 8(long) + utils.IntSize(int) + (utils.IntSize + 3*4)(varchar) + 1(bool) + padding
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := tt.schemaBuilder()
			layout := NewLayout(schema)

			assert.Equal(t, tt.expectedSize, layout.SlotSize(),
				"Layout size not optimally minimized")

			// Verify all fields are properly aligned
			for _, field := range schema.Fields() {
				offset := layout.Offset(field)
				alignment := alignmentRequirement(schema.Type(field))
				assert.Equal(t, 0, offset%alignment,
					"Field %s not properly aligned. Offset: %d, Required alignment: %d",
					field, offset, alignment)
			}
		})
	}
}
