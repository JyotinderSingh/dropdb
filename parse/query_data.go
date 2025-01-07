package parse

import "github.com/JyotinderSingh/dropdb/query"

type QueryData struct {
	fields    []string
	tables    []string
	predicate *query.Predicate
}

func NewQueryData(fields, tables []string, predicate *query.Predicate) *QueryData {
	return &QueryData{
		fields:    fields,
		tables:    tables,
		predicate: predicate,
	}
}

func (qd *QueryData) Fields() []string {
	return qd.fields
}

func (qd *QueryData) Tables() []string {
	return qd.tables
}

func (qd *QueryData) Pred() *query.Predicate {
	return qd.predicate
}

func (qd *QueryData) String() string {
	if len(qd.fields) == 0 || len(qd.tables) == 0 {
		return ""
	}
	result := "select "
	for _, fieldName := range qd.fields {
		result += fieldName + ", "
	}
	// remove final comma/space
	if len(qd.fields) > 0 {
		result = result[:len(result)-2]
	}
	result += " from "
	for _, tableName := range qd.tables {
		result += tableName + ", "
	}
	if len(qd.tables) > 0 {
		result = result[:len(result)-2]
	}
	predicateString := qd.predicate.String()
	if predicateString != "" {
		result += " where " + predicateString
	}
	return result
}
