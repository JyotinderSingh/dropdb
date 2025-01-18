package plan_impl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/JyotinderSingh/dropdb/scan"
)

func TestProjectPlan_Basic(t *testing.T) {
	// 1) Setup environment
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	// 2) Create table "users" with three fields
	mdm := createTableWithSchema(t, txn, "users", map[string]interface{}{
		"id":     0,
		"name":   "string",
		"active": true,
	})

	// 3) Create a TablePlan to insert and read from "users"
	tp, err := NewTablePlan(txn, "users", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	// Insert some test data
	records := []map[string]interface{}{
		{"id": 1, "name": "Alice", "active": true},
		{"id": 2, "name": "Bob", "active": false},
		{"id": 3, "name": "Carol", "active": true},
	}
	insertRecords(t, us, records)

	// Re-instantiate TablePlan after insertion to refresh stats
	tp, err = NewTablePlan(txn, "users", mdm)
	require.NoError(t, err)

	// 4) Create a ProjectPlan selecting only ["id", "name"]
	projectedFields := []string{"id", "name"}
	pp, err := NewProjectPlan(tp, projectedFields)
	require.NoError(t, err)

	// 5) Open the ProjectPlan and verify records
	projectScan, err := pp.Open()
	require.NoError(t, err)
	defer projectScan.Close()

	require.NoError(t, projectScan.BeforeFirst())

	readCount := 0
	for {
		hasNext, err := projectScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		readCount++

		// We should be able to read 'id' and 'name'
		userID, err := projectScan.GetInt("id")
		require.NoError(t, err)
		userName, err := projectScan.GetString("name")
		require.NoError(t, err)

		// But 'active' is not in the projection, so it should fail:
		// either it returns an error or you must not call GetBool("active").
		// We'll just check the schema instead:
		hasActive := pp.Schema().HasField("active")
		assert.False(t, hasActive, "Schema should NOT include 'active' in projection")

		// Ensure returned values match one of the inserted rows
		// (id, name) among our test records.
		var found bool
		for _, rec := range records {
			if rec["id"] == userID && rec["name"] == userName {
				found = true
				break
			}
		}
		assert.True(t, found, "Projected (id,name) should match an inserted record")
	}
	assert.Equal(t, len(records), readCount, "Projected scan should return all rows, but only 2 fields")

	// 6) Validate plan-level stats
	// ProjectPlan does not change #blocks accessed or #records; it only hides fields.
	assert.Equal(t, tp.BlocksAccessed(), pp.BlocksAccessed())
	assert.Equal(t, tp.RecordsOutput(), pp.RecordsOutput())

	// Distinct values for "id" is the same as the underlying plan's estimate
	distinctID := pp.DistinctValues("id")
	assert.Equal(t, tp.DistinctValues("id"), distinctID)

	// 7) Validate the projected schema
	schema := pp.Schema()
	require.NotNil(t, schema)
	assert.True(t, schema.HasField("id"))
	assert.True(t, schema.HasField("name"))
	assert.False(t, schema.HasField("active"))
	assert.Len(t, schema.Fields(), 2, "Schema should only have 2 fields in the projection")
}
