package record

import (
	"github.com/JyotinderSingh/dropdb/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddField(t *testing.T) {
	s := NewSchema()

	tests := []struct {
		name   string
		field  string
		typ    types.SchemaType
		length int
	}{
		{"integer field", "age", types.Integer, 0},
		{"varchar field", "name", types.Varchar, 20},
		{"boolean field", "active", types.Boolean, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s.AddField(tt.field, tt.typ, tt.length)

			info, ok := s.info[tt.field]
			assert.True(t, ok, "Field %s not found in info map", tt.field)
			assert.Equal(t, tt.typ, info.Type, "Field type mismatch")
			assert.Equal(t, tt.length, info.Length, "Field length mismatch")
		})
	}
}

func TestTypeSpecificAdders(t *testing.T) {
	s := NewSchema()

	tests := []struct {
		name     string
		adder    func()
		field    string
		expected types.SchemaType
		length   int
	}{
		{
			"AddIntField",
			func() { s.AddIntField("age") },
			"age",
			types.Integer,
			0,
		},
		{
			"AddStringField",
			func() { s.AddStringField("name", 30) },
			"name",
			types.Varchar,
			30,
		},
		{
			"AddBoolField",
			func() { s.AddBoolField("active") },
			"active",
			types.Boolean,
			0,
		},
		{
			"AddLongField",
			func() { s.AddLongField("id") },
			"id",
			types.Long,
			0,
		},
		{
			"AddShortField",
			func() { s.AddShortField("count") },
			"count",
			types.Short,
			0,
		},
		{
			"AddDateField",
			func() { s.AddDateField("created") },
			"created",
			types.Date,
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.adder()

			assert.True(t, s.HasField(tt.field), "Field %s not found", tt.field)
			assert.Equal(t, tt.expected, s.Type(tt.field), "Field type mismatch")
			assert.Equal(t, tt.length, s.Length(tt.field), "Field length mismatch")
		})
	}
}

func TestAdd(t *testing.T) {
	source := &Schema{
		fields: []string{"id", "name"},
		info: map[string]types.FieldInfo{
			"id":   {types.Integer, 0},
			"name": {types.Varchar, 25},
		},
	}

	dest := NewSchema()

	// Add individual fields
	dest.Add("id", source)
	dest.Add("name", source)

	assert.Equal(t, 2, len(dest.fields), "Expected 2 fields")

	// Check id field
	idInfo, ok := dest.info["id"]
	assert.True(t, ok, "id field not found")
	assert.Equal(t, types.Integer, idInfo.Type, "id field type mismatch")
	assert.Equal(t, 0, idInfo.Length, "id field length mismatch")

	// Check name field
	nameInfo, ok := dest.info["name"]
	assert.True(t, ok, "name field not found")
	assert.Equal(t, types.Varchar, nameInfo.Type, "name field type mismatch")
	assert.Equal(t, 25, nameInfo.Length, "name field length mismatch")
}

func TestAddAll(t *testing.T) {
	source := &Schema{
		fields: []string{"id", "name", "active"},
		info: map[string]types.FieldInfo{
			"id":     {types.Integer, 0},
			"name":   {types.Varchar, 25},
			"active": {types.Boolean, 0},
		},
	}

	dest := NewSchema()

	dest.AddAll(source)

	assert.Equal(t, len(source.fields), len(dest.fields), "Field count mismatch")

	// Verify field order is preserved
	for i, field := range source.fields {
		assert.Equal(t, field, dest.fields[i], "Field order mismatch at index %d", i)
	}

	// Verify all field info was copied correctly
	for field, sourceInfo := range source.info {
		destInfo, ok := dest.info[field]
		assert.True(t, ok, "Field %s not found in destination schema", field)
		assert.Equal(t, sourceInfo, destInfo, "Field info mismatch for %s", field)
	}
}
