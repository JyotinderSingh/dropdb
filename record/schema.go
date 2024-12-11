package record

// Schema represents record schema of a table.
// A schema contains the name and type of each
// field of the table, as well as the length of
// each varchar field.
type Schema struct {
	fields []string
	info   map[string]FieldInfo
}

// NewSchema creates a new schema.
func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

// AddField adds a field to the schema having a specified
// name, type, and length.
// If the field type is not a character type, the length
// value is irrelevant.
func (s *Schema) AddField(fieldName string, fieldType SchemaType, length int) {
	s.fields = append(s.fields, fieldName)
	s.info[fieldName] = FieldInfo{fieldType, length}
}

// AddIntField adds an integer field to the schema.
func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, Integer, 0)
}

// AddStringField adds a string field to the schema.
func (s *Schema) AddStringField(fieldName string, length int) {
	s.AddField(fieldName, Varchar, length)
}

// AddBoolField adds a boolean field to the schema.
func (s *Schema) AddBoolField(fieldName string) {
	s.AddField(fieldName, Boolean, 0)
}

// AddLongField adds a long field to the schema.
func (s *Schema) AddLongField(fieldName string) {
	s.AddField(fieldName, Long, 0)
}

// AddShortField adds a short field to the schema.
func (s *Schema) AddShortField(fieldName string) {
	s.AddField(fieldName, Short, 0)
}

// AddDateField adds a date field to the schema.
func (s *Schema) AddDateField(fieldName string) {
	s.AddField(fieldName, Date, 0)
}

// Add adds a field to the schema having the same
// type and length as the corresponding field in
// the specified schema.
func (s *Schema) Add(fieldName string, other *Schema) {
	info := other.info[fieldName]
	s.AddField(fieldName, info.fieldType, info.length)
}

// AddAll adds all the fields in the specified schema to the current schema.
func (s *Schema) AddAll(other *Schema) {
	for _, field := range other.fields {
		s.Add(field, other)
	}
}

// Fields returns the names of all the fields in the schema.
func (s *Schema) Fields() []string {
	return s.fields
}

// HasField returns true if the schema contains a field with the specified name.
func (s *Schema) HasField(fieldName string) bool {
	_, ok := s.info[fieldName]
	return ok
}

// Type returns the type of the field with the specified name.
func (s *Schema) Type(fieldName string) SchemaType {
	return s.info[fieldName].fieldType
}

// Length returns the length of the field with the specified name.
func (s *Schema) Length(fieldName string) int {
	return s.info[fieldName].length
}
