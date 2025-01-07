package parse

type InsertData struct {
	tableName string
	fields    []string
	values    []any
}

func NewInsertData(tableName string, fields []string, values []any) *InsertData {
	return &InsertData{
		tableName: tableName,
		fields:    fields,
		values:    values,
	}
}

func (id *InsertData) TableName() string {
	return id.tableName
}

func (id *InsertData) Fields() []string {
	return id.fields
}

func (id *InsertData) Values() []any {
	return id.values
}
