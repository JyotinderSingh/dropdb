package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/parse"
	"github.com/JyotinderSingh/dropdb/tx"
)

type UpdatePlanner interface {
	// ExecuteInsert executes the specified insert statement, and
	// returns the numbeb of affected records.
	ExecuteInsert(data *parse.InsertData, transaction *tx.Transaction) (int, error)

	// ExecuteDelete executes the specified delete statement, and
	// returns the number of affected records.
	ExecuteDelete(data *parse.DeleteData, transaction *tx.Transaction) (int, error)

	// ExecuteModify executes the specified modify statement, and
	// returns the number of affected records.
	ExecuteModify(data *parse.ModifyData, transaction *tx.Transaction) (int, error)

	// ExecuteCreateTable executes the specified create table statement, and
	// returns the number of affected records.
	ExecuteCreateTable(data *parse.CreateTableData, transaction *tx.Transaction) (int, error)

	// ExecuteCreateView executes the specified create view statement, and
	// returns the number of affected records.
	ExecuteCreateView(data *parse.CreateViewData, transaction *tx.Transaction) (int, error)

	// ExecuteCreateIndex executes the specified create index statement, and
	// returns the number of affected records.
	ExecuteCreateIndex(data *parse.CreateIndexData, transaction *tx.Transaction) (int, error)
}
