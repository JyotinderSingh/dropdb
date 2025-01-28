package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/parse"
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/tx"
)

var _ QueryPlanner = &BasicQueryPlanner{}

type BasicQueryPlanner struct {
	metadataManager *metadata.Manager
}

// NewBasicQueryPlanner creates a new BasicQueryPlanner
func NewBasicQueryPlanner(metadataManager *metadata.Manager) *BasicQueryPlanner {
	return &BasicQueryPlanner{metadataManager: metadataManager}
}

// CreatePlan creates a query plan as follows:
// 1. Takes the product of all tables and views
// 2. Applies predicate selection
// 3. Applies grouping and having if specified
// 4. Projects on the field list
// 5. Applies ordering if specified
func (qp *BasicQueryPlanner) CreatePlan(queryData *parse.QueryData, transaction *tx.Transaction) (plan.Plan, error) {
	// 1. Create a plan for each mentioned table or view
	plans := make([]plan.Plan, len(queryData.Tables()))
	for idx, tableName := range queryData.Tables() {
		viewDefinition, err := qp.metadataManager.GetViewDefinition(tableName, transaction)
		if err != nil {
			return nil, err
		}

		if viewDefinition == "" {
			tablePlan, err := NewTablePlan(transaction, tableName, qp.metadataManager)
			if err != nil {
				return nil, err
			}
			plans[idx] = tablePlan
		} else {
			parser := parse.NewParser(viewDefinition)
			viewData, err := parser.Query()
			if err != nil {
				return nil, err
			}

			viewPlan, err := qp.CreatePlan(viewData, transaction)
			if err != nil {
				return nil, err
			}
			plans[idx] = viewPlan
		}
	}

	// 2. Create the product of all table plans
	var err error
	currentPlan := plans[0]
	plans = plans[1:]

	for _, nextPlan := range plans {
		planChoice1, err := NewProductPlan(currentPlan, nextPlan)
		if err != nil {
			return nil, err
		}

		planChoice2, err := NewProductPlan(nextPlan, currentPlan)
		if err != nil {
			return nil, err
		}

		if planChoice1.BlocksAccessed() < planChoice2.BlocksAccessed() {
			currentPlan = planChoice1
		} else {
			currentPlan = planChoice2
		}
	}

	// 3. Add a selection plan for the predicate
	currentPlan = NewSelectPlan(currentPlan, queryData.Pred())

	projectionFields := queryData.Fields()
	// 4. Add grouping if specified
	if len(queryData.GroupBy()) > 0 {
		currentPlan = NewGroupByPlan(transaction, currentPlan, queryData.GroupBy(), queryData.Aggregates())

		// Apply having clause if present
		if queryData.Having() != nil {
			currentPlan = NewSelectPlan(currentPlan, queryData.Having())
		}

		for _, AggFunc := range queryData.Aggregates() {
			projectionFields = append(projectionFields, AggFunc.FieldName())
		}
	}

	// 5. Add a projection plan for the field list
	currentPlan, err = NewProjectPlan(currentPlan, projectionFields)
	if err != nil {
		return nil, err
	}

	// 6. Add ordering if specified
	if len(queryData.OrderBy()) > 0 {
		sortFields := make([]string, len(queryData.OrderBy()))
		for i, item := range queryData.OrderBy() {
			// Note: Currently the SortPlan doesn't support descending order
			sortFields[i] = item.Field()
		}
		currentPlan = NewSortPlan(transaction, currentPlan, sortFields)
	}

	return currentPlan, nil
}
