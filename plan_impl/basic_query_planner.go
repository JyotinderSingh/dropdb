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
// It first takes the product ofo all tables and views.
// It then selects on the predicate.
// And finally it projects on the field list.
func (qp *BasicQueryPlanner) CreatePlan(queryData *parse.QueryData, transaction *tx.Transaction) (plan.Plan, error) {
	// 1. Create a plan for each mentioned table or view.
	plans := make([]plan.Plan, len(queryData.Tables()))
	for idx, tableName := range queryData.Tables() {
		viewDefinition, err := qp.metadataManager.GetViewDefinition(tableName, transaction)
		if err != nil {
			return nil, err
		}

		// If the table is not a view, create a plan for it.
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
		// Generate two alternative plans for the product of the two plans.
		planChoice1, err := NewProductPlan(currentPlan, nextPlan)
		if err != nil {
			return nil, err
		}

		planChoice2, err := NewProductPlan(nextPlan, currentPlan)
		if err != nil {
			return nil, err
		}

		// Choose the plan with the lower cost.
		if planChoice1.BlocksAccessed() < planChoice2.BlocksAccessed() {
			currentPlan = planChoice1
		} else {
			currentPlan = planChoice2
		}
	}

	// 3. Add a selection plan for the predicate.
	currentPlan = NewSelectPlan(currentPlan, queryData.Pred())

	// 4. Add a projection plan for the field list.
	currentPlan, err = NewProjectPlan(currentPlan, queryData.Fields())
	if err != nil {
		return nil, err
	}

	return currentPlan, nil
}
