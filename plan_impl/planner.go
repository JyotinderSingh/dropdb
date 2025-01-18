package plan_impl

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/parse"
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/tx"
)

type Planner struct {
	queryPlanner  QueryPlanner
	updatePlanner UpdatePlanner
}

func NewPlanner(queryPlanner QueryPlanner, updatePlanner UpdatePlanner) *Planner {
	return &Planner{
		queryPlanner:  queryPlanner,
		updatePlanner: updatePlanner,
	}
}

// CreateQueryPlan creates a plan for a SQL select statement, using the supplied planner.
func (planner *Planner) CreateQueryPlan(sql string, transaction *tx.Transaction) (plan.Plan, error) {
	parser := parse.NewParser(sql)
	data, err := parser.Query()
	if err != nil {
		return nil, err
	}
	if err := verifyQuery(data); err != nil {
		return nil, err
	}
	return planner.queryPlanner.CreatePlan(data, transaction)
}

// ExecuteUpdate executes a SQL insert, delete, modify, or create statement.
// The method dispatches to the appropriate method of the supplied update planner,
// depending on what the parser returns.
func (planner *Planner) ExecuteUpdate(sql string, transaction *tx.Transaction) (int, error) {
	parser := parse.NewParser(sql)
	data, err := parser.UpdateCmd()
	if err != nil {
		return 0, err
	}

	if err := verifyUpdate(data); err != nil {
		return 0, err
	}

	switch data.(type) {
	case *parse.InsertData:
		return planner.updatePlanner.ExecuteInsert(data.(*parse.InsertData), transaction)
	case *parse.DeleteData:
		return planner.updatePlanner.ExecuteDelete(data.(*parse.DeleteData), transaction)
	case *parse.ModifyData:
		return planner.updatePlanner.ExecuteModify(data.(*parse.ModifyData), transaction)
	case *parse.CreateTableData:
		return planner.updatePlanner.ExecuteCreateTable(data.(*parse.CreateTableData), transaction)
	case *parse.CreateViewData:
		return planner.updatePlanner.ExecuteCreateView(data.(*parse.CreateViewData), transaction)
	case *parse.CreateIndexData:
		return planner.updatePlanner.ExecuteCreateIndex(data.(*parse.CreateIndexData), transaction)
	default:
		return 0, fmt.Errorf("unexpected type %T", data)
	}
}

func verifyQuery(data *parse.QueryData) error {
	// TODO: Implement this
	return nil
}

func verifyUpdate(data any) error {
	// TODO: Implement this
	return nil
}
