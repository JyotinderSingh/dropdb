package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/parse"
	"github.com/JyotinderSingh/dropdb/plan"
	"github.com/JyotinderSingh/dropdb/tx"
)

// QueryPlanner is an interface implemented by planners for the SQL select statement.
type QueryPlanner interface {
	// CreatePlan creates a query plan for the specified query data.
	CreatePlan(queryData *parse.QueryData, transaction *tx.Transaction) (plan.Plan, error)
}
