package parse

type CreateIndexData struct {
	indexName string
	tableName string
	fieldName string
}

func NewCreateIndexData(indexName, tableName, fieldName string) *CreateIndexData {
	return &CreateIndexData{
		indexName: indexName,
		tableName: tableName,
		fieldName: fieldName,
	}
}

func (cid *CreateIndexData) IndexName() string {
	return cid.indexName
}

func (cid *CreateIndexData) TableName() string {
	return cid.tableName
}

func (cid *CreateIndexData) FieldName() string {
	return cid.fieldName
}
