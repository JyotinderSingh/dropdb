package parse

import "github.com/JyotinderSingh/dropdb/query"

type ModifyData struct {
	tableName string
	fieldName string
	newValue  *query.Expression
	predicate *query.Predicate
}

func NewModifyData(tableName, fieldName string, newVal *query.Expression, pred *query.Predicate) *ModifyData {
	return &ModifyData{
		tableName: tableName,
		fieldName: fieldName,
		newValue:  newVal,
		predicate: pred,
	}
}

func (md *ModifyData) TableName() string {
	return md.tableName
}

func (md *ModifyData) TargetField() string {
	return md.fieldName
}

func (md *ModifyData) NewValue() *query.Expression {
	return md.newValue
}

func (md *ModifyData) Predicate() *query.Predicate {
	return md.predicate
}
