package parse

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/query"
	"github.com/JyotinderSingh/dropdb/query/functions"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/types"
	"strings"
)

type Parser struct {
	lex *Lexer
}

func NewParser(s string) *Parser {
	return &Parser{
		lex: NewLexer(s),
	}
}

// -- Predicates, terms, expressions, constants, fields --

func (p *Parser) field() (string, error) {
	fld, err := p.lex.EatId()
	if err != nil {
		return "", err
	}
	return fld, nil
}

func (p *Parser) constant() (any, error) {
	if p.lex.MatchStringConstant() {
		stringVal, err := p.lex.EatStringConstant()
		if err != nil {
			return nil, err
		}
		return stringVal, nil
	}
	if p.lex.MatchIntConstant() {
		intVal, err := p.lex.EatIntConstant()
		if err != nil {
			return nil, err
		}
		return intVal, nil
	}
	if p.lex.MatchBooleanConstant() {
		boolVal, err := p.lex.EatBooleanConstant()
		if err == nil {
			return boolVal, nil
		}
		return boolVal, nil
	}
	if p.lex.MatchDateConstant() {
		dateVal, err := p.lex.EatDateConstant()
		if err == nil {
			return dateVal, nil
		}
		return dateVal, nil
	}
	return nil, &SyntaxError{Message: "expected constant"}
}

func (p *Parser) expression() (*query.Expression, error) {
	// Check for aggregate function first
	if p.lex.MatchKeyword("max") || p.lex.MatchKeyword("min") ||
		p.lex.MatchKeyword("count") || p.lex.MatchKeyword("avg") ||
		p.lex.MatchKeyword("sum") {
		agg, err := p.parseAggregate()
		if err != nil {
			return nil, err
		}
		return query.NewFieldExpression(agg.FieldName()), nil
	}

	// If next token is an identifier, treat as field
	if p.lex.MatchId() {
		f, err := p.field()
		if err != nil {
			return &query.Expression{}, err
		}
		return query.NewFieldExpression(f), nil
	}

	// Otherwise treat as constant
	c, err := p.constant()
	if err != nil {
		return &query.Expression{}, err
	}
	return query.NewConstantExpression(c), nil
}

func (p *Parser) term() (*query.Term, error) {
	// Left-hand side expression
	lhs, err := p.expression()
	if err != nil {
		return &query.Term{}, err
	}

	// Read the operator from the lexer
	op, err := p.parseOperator()
	if err != nil {
		return &query.Term{}, err
	}

	// Right-hand side expression
	rhs, err := p.expression()
	if err != nil {
		return &query.Term{}, err
	}

	parsedOp, err := types.OperatorFromString(op)
	if err != nil {
		return &query.Term{}, err
	}

	// Construct the Term with the operator
	return query.NewTerm(lhs, rhs, parsedOp), nil
}

func (p *Parser) parseOperator() (string, error) {
	// Ensure the current token is indeed an operator
	if p.lex.currentToken.Type != TTOperator {
		return "", &SyntaxError{Message: "expected comparison operator (e.g. =, >, >=, etc.)"}
	}
	// Grab the operator text, e.g. "=", ">=", "<=", "!="...
	op := p.lex.currentToken.StringVal

	// Move to the next token
	if err := p.lex.nextToken(); err != nil {
		return "", err
	}
	return op, nil
}

func (p *Parser) predicate() (*query.Predicate, error) {
	firstTerm, err := p.term()
	if err != nil {
		return &query.Predicate{}, err
	}
	pred := query.NewPredicateFromTerm(firstTerm)

	// check if there's an "and"
	if p.lex.MatchKeyword("and") {
		_ = p.lex.EatKeyword("and") // ignoring error for brevity
		otherPred, err := p.predicate()
		if err != nil {
			return &query.Predicate{}, err
		}
		pred.ConjoinWith(otherPred)
	}
	return pred, nil
}

// -- Queries --

func (p *Parser) Query() (*QueryData, error) {
	// "select"
	if err := p.lex.EatKeyword("select"); err != nil {
		return nil, err
	}

	// Parse fields and aggregates
	fields, aggregates, err := p.selectList()
	if err != nil {
		return nil, err
	}

	// "from"
	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}
	tables, err := p.tableList()
	if err != nil {
		return nil, err
	}

	// Initialize predicate
	pred := query.NewPredicate()

	// Optional "where"
	if p.lex.MatchKeyword("where") {
		_ = p.lex.EatKeyword("where")
		pr, err := p.predicate()
		if err != nil {
			return nil, err
		}
		pred = pr
	}

	// Optional "group by"
	var groupBy []string
	if p.lex.MatchKeyword("group") {
		groupBy, err = p.parseGroupBy()
		if err != nil {
			return nil, err
		}
	}

	// Optional "having"
	var having *query.Predicate
	if p.lex.MatchKeyword("having") {
		_ = p.lex.EatKeyword("having")
		having, err = p.predicate()
		if err != nil {
			return nil, err
		}
	}

	// Optional "order by"
	var orderBy []OrderByItem
	if p.lex.MatchKeyword("order") {
		orderBy, err = p.parseOrderBy()
		if err != nil {
			return nil, err
		}
	}

	return &QueryData{
		fields:     fields,
		tables:     tables,
		predicate:  pred,
		groupBy:    groupBy,
		having:     having,
		orderBy:    orderBy,
		aggregates: aggregates,
	}, nil
}

func (p *Parser) selectList() ([]string, []functions.AggregationFunction, error) {
	var fields []string
	var aggregates []functions.AggregationFunction

	for {
		// Check for aggregate function
		if p.lex.MatchKeyword("max") || p.lex.MatchKeyword("min") ||
			p.lex.MatchKeyword("count") || p.lex.MatchKeyword("avg") ||
			p.lex.MatchKeyword("sum") {
			agg, err := p.parseAggregate()
			if err != nil {
				return nil, nil, err
			}
			aggregates = append(aggregates, agg)
			// I don't think this is needed, might uncomment later :P
			//fields = append(fields, agg.FieldName())
		} else {
			// Regular field
			field, err := p.field()
			if err != nil {
				return nil, nil, err
			}
			fields = append(fields, field)
		}

		// Continue if there's a comma
		if !p.lex.MatchDelim(',') {
			break
		}
		_ = p.lex.EatDelim(',')
	}

	return fields, aggregates, nil
}

// Parse aggregate function
func (p *Parser) parseAggregate() (functions.AggregationFunction, error) {
	// Get function name
	funcName := strings.ToLower(p.lex.currentToken.StringVal)
	if err := p.lex.nextToken(); err != nil {
		return nil, err
	}

	// Expect opening parenthesis
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}

	// Get field name
	field, err := p.field()
	if err != nil {
		return nil, err
	}

	// Expect closing parenthesis
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}

	switch funcName {
	case "max":
		return functions.NewMaxFunction(field), nil
	case "min":
		return functions.NewMinFunction(field), nil
	case "count":
		return functions.NewCountFunction(field), nil
	case "avg":
		return functions.NewAvgFunction(field), nil
	case "sum":
		return functions.NewSumFunction(field), nil
	default:
		return nil, fmt.Errorf("unknown aggregate function: %s", funcName)
	}
}

// Parse GROUP BY clause
func (p *Parser) parseGroupBy() ([]string, error) {
	if err := p.lex.EatKeyword("group"); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("by"); err != nil {
		return nil, err
	}

	return p.fieldList()
}

// Parse ORDER BY clause
func (p *Parser) parseOrderBy() ([]OrderByItem, error) {
	if err := p.lex.EatKeyword("order"); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("by"); err != nil {
		return nil, err
	}

	var items []OrderByItem
	for {
		var field string
		var err error

		// Check for aggregate function
		if p.lex.MatchKeyword("max") || p.lex.MatchKeyword("min") ||
			p.lex.MatchKeyword("count") || p.lex.MatchKeyword("avg") ||
			p.lex.MatchKeyword("sum") {
			agg, err := p.parseAggregate()
			if err != nil {
				return nil, err
			}
			field = agg.FieldName()
		} else {
			field, err = p.field()
			if err != nil {
				return nil, err
			}
		}

		// Check for optional ASC/DESC
		descending := false
		if p.lex.MatchKeyword("desc") {
			_ = p.lex.EatKeyword("desc")
			descending = true
		} else if p.lex.MatchKeyword("asc") {
			_ = p.lex.EatKeyword("asc")
		}

		items = append(items, OrderByItem{field: field, descending: descending})

		if !p.lex.MatchDelim(',') {
			break
		}
		_ = p.lex.EatDelim(',')
	}

	return items, nil
}

func (p *Parser) tableList() ([]string, error) {
	t, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	tables := []string{t}
	if p.lex.MatchDelim(',') {
		_ = p.lex.EatDelim(',')
		rest, err := p.tableList()
		if err != nil {
			return nil, err
		}
		tables = append(tables, rest...)
	}
	return tables, nil
}

// -- Update Commands --

func (p *Parser) UpdateCmd() (interface{}, error) {
	if p.lex.MatchKeyword("insert") {
		return p.insert()
	} else if p.lex.MatchKeyword("delete") {
		return p.delete()
	} else if p.lex.MatchKeyword("update") {
		return p.modify()
	} else {
		return p.create()
	}
}

func (p *Parser) create() (interface{}, error) {
	if err := p.lex.EatKeyword("create"); err != nil {
		return nil, err
	}
	if p.lex.MatchKeyword("table") {
		return p.createTable()
	} else if p.lex.MatchKeyword("view") {
		return p.createView()
	} else {
		return p.createIndex()
	}
}

// -- Delete Commands --

func (p *Parser) delete() (*DeleteData, error) {
	if err := p.lex.EatKeyword("delete"); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("from"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate()
	if p.lex.MatchKeyword("where") {
		_ = p.lex.EatKeyword("where")
		pr, err := p.predicate()
		if err != nil {
			return nil, err
		}
		pred = pr
	}
	return NewDeleteData(tableName, pred), nil
}

// -- Insert Commands --

func (p *Parser) insert() (*InsertData, error) {
	if err := p.lex.EatKeyword("insert"); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("into"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}
	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("values"); err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}
	vals, err := p.constList()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}
	return NewInsertData(tableName, fields, vals), nil
}

func (p *Parser) fieldList() ([]string, error) {
	f, err := p.field()
	if err != nil {
		return nil, err
	}
	fields := []string{f}
	if p.lex.MatchDelim(',') {
		_ = p.lex.EatDelim(',')
		rest, err := p.fieldList()
		if err != nil {
			return nil, err
		}
		fields = append(fields, rest...)
	}
	return fields, nil
}

func (p *Parser) constList() ([]any, error) {
	c, err := p.constant()
	if err != nil {
		return nil, err
	}
	vals := []any{c}
	if p.lex.MatchDelim(',') {
		_ = p.lex.EatDelim(',')
		rest, err := p.constList()
		if err != nil {
			return nil, err
		}
		vals = append(vals, rest...)
	}
	return vals, nil
}

// -- Modify Commands --

func (p *Parser) modify() (*ModifyData, error) {
	if err := p.lex.EatKeyword("update"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("set"); err != nil {
		return nil, err
	}
	fieldName, err := p.field()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatOperator("="); err != nil {
		return nil, err
	}
	newVal, err := p.expression()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate()
	if p.lex.MatchKeyword("where") {
		_ = p.lex.EatKeyword("where")
		pr, err := p.predicate()
		if err != nil {
			return nil, err
		}
		pred = pr
	}
	return NewModifyData(tableName, fieldName, newVal, pred), nil
}

// -- Create Table Commands --

func (p *Parser) createTable() (*CreateTableData, error) {
	if err := p.lex.EatKeyword("table"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}
	sch, err := p.fieldDefs()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}
	return NewCreateTableData(tableName, sch), nil
}

func (p *Parser) fieldDefs() (*record.Schema, error) {
	schema, err := p.fieldDef()
	if err != nil {
		return nil, err
	}
	if p.lex.MatchDelim(',') {
		_ = p.lex.EatDelim(',')
		schema2, err := p.fieldDefs()
		if err != nil {
			return nil, err
		}
		schema.AddAll(schema2)
	}
	return schema, nil
}

func (p *Parser) fieldDef() (*record.Schema, error) {
	fieldName, err := p.field()
	if err != nil {
		return nil, err
	}
	return p.fieldType(fieldName)
}

func (p *Parser) fieldType(fieldName string) (*record.Schema, error) {
	schema := record.NewSchema()

	switch {
	case p.lex.MatchKeyword("int"):
		_ = p.lex.EatKeyword("int")
		schema.AddIntField(fieldName)

	case p.lex.MatchKeyword("varchar"):
		_ = p.lex.EatKeyword("varchar")
		if err := p.parseVarcharLength(fieldName, schema); err != nil {
			return nil, err
		}

	case p.lex.MatchKeyword("bool"):
		_ = p.lex.EatKeyword("bool")
		schema.AddBoolField(fieldName)

	case p.lex.MatchKeyword("date"):
		_ = p.lex.EatKeyword("date")
		schema.AddDateField(fieldName)

	default:
		return nil, &SyntaxError{Message: "expected field type"}
	}

	return schema, nil
}

func (p *Parser) parseVarcharLength(fieldName string, schema *record.Schema) error {
	_ = p.lex.EatDelim('(')
	length, err := p.lex.EatIntConstant()
	if err != nil {
		return err
	}
	_ = p.lex.EatDelim(')')
	schema.AddStringField(fieldName, length)
	return nil
}

// -- Create View Commands --

func (p *Parser) createView() (*CreateViewData, error) {
	if err := p.lex.EatKeyword("view"); err != nil {
		return nil, err
	}
	viewName, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("as"); err != nil {
		return nil, err
	}
	qd, err := p.Query()
	if err != nil {
		return nil, err
	}
	return NewCreateViewData(viewName, qd), nil
}

// -- Create Index Commands --

func (p *Parser) createIndex() (*CreateIndexData, error) {
	if err := p.lex.EatKeyword("index"); err != nil {
		return nil, err
	}
	indexName, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatKeyword("on"); err != nil {
		return nil, err
	}
	tableName, err := p.lex.EatId()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim('('); err != nil {
		return nil, err
	}
	fieldName, err := p.field()
	if err != nil {
		return nil, err
	}
	if err := p.lex.EatDelim(')'); err != nil {
		return nil, err
	}
	return NewCreateIndexData(indexName, tableName, fieldName), nil
}
