package parse

import "github.com/JyotinderSingh/dropdb/query"

type DeleteData struct {
	tableName string
	predicate *query.Predicate
}

func NewDeleteData(tableName string, predicate *query.Predicate) *DeleteData {
	return &DeleteData{
		tableName: tableName,
		predicate: predicate,
	}
}

func (dd *DeleteData) TableName() string {
	return dd.tableName
}

func (dd *DeleteData) Predicate() *query.Predicate {
	return dd.predicate
}
