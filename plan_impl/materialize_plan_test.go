package plan_impl

import (
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMaterializePlan_Basic(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	// Create source table and data
	mdm := createTableMetadataWithSchema(t, txn, "employees", map[string]interface{}{
		"id":     0,
		"name":   "string",
		"salary": 0,
	})

	tp, err := NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	testData := []map[string]interface{}{
		{"id": 1, "name": "Alice", "salary": 50000},
		{"id": 2, "name": "Bob", "salary": 60000},
		{"id": 3, "name": "Carol", "salary": 70000},
	}
	insertRecords(t, us, testData)

	// re-initialize the table plan to refresh the statistics
	tp, err = NewTablePlan(txn, "employees", mdm)
	require.NoError(t, err)

	// Create MaterializePlan
	mp := NewMaterializePlan(txn, tp)

	// Test materialization
	matScan, err := mp.Open()
	require.NoError(t, err)
	defer matScan.Close()

	// Verify all records were materialized correctly
	require.NoError(t, matScan.BeforeFirst())
	count := 0
	for {
		hasNext, err := matScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		id, err := matScan.GetInt("id")
		require.NoError(t, err)
		name, err := matScan.GetString("name")
		require.NoError(t, err)
		salary, err := matScan.GetInt("salary")
		require.NoError(t, err)

		assert.Equal(t, testData[count]["id"], id)
		assert.Equal(t, testData[count]["name"], name)
		assert.Equal(t, testData[count]["salary"], salary)
		count++
	}
	assert.Equal(t, len(testData), count)

	// Test plan statistics
	assert.Equal(t, tp.RecordsOutput(), mp.RecordsOutput())
	assert.Equal(t, tp.Schema(), mp.Schema())
	assert.Equal(t, tp.DistinctValues("name"), mp.DistinctValues("name"))
	assert.True(t, mp.BlocksAccessed() > 0)
}

func TestMaterializePlan_EmptySource(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "empty_table", map[string]interface{}{
		"id":   0,
		"name": "string",
	})

	tp, err := NewTablePlan(txn, "empty_table", mdm)
	require.NoError(t, err)

	mp := NewMaterializePlan(txn, tp)
	matScan, err := mp.Open()
	require.NoError(t, err)
	defer matScan.Close()

	require.NoError(t, matScan.BeforeFirst())
	hasNext, err := matScan.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

func TestMaterializePlan_LargeDataset(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "large_table", map[string]interface{}{
		"id":  0,
		"val": 0,
	})

	tp, err := NewTablePlan(txn, "large_table", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	// Insert 1000 records
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		require.NoError(t, us.Insert())
		require.NoError(t, us.SetInt("id", i))
		require.NoError(t, us.SetInt("val", i*10))
	}

	// re-initialize the table plan to refresh the statistics
	tp, err = NewTablePlan(txn, "large_table", mdm)
	require.NoError(t, err)

	mp := NewMaterializePlan(txn, tp)
	matScan, err := mp.Open()
	require.NoError(t, err)
	defer matScan.Close()

	// Verify materialization
	require.NoError(t, matScan.BeforeFirst())
	count := 0
	for {
		hasNext, err := matScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		id, err := matScan.GetInt("id")
		require.NoError(t, err)
		val, err := matScan.GetInt("val")
		require.NoError(t, err)

		assert.Equal(t, count, id)
		assert.Equal(t, count*10, val)
		count++
	}
	assert.Equal(t, recordCount, count)

	// Verify BlocksAccessed estimation
	layout := record.NewLayout(mp.Schema())
	recordsPerBlock := txn.BlockSize() / layout.SlotSize()
	expectedBlocks := (recordCount + recordsPerBlock - 1) / recordsPerBlock
	assert.InDelta(t, expectedBlocks, mp.BlocksAccessed(), 2)
}

func TestMaterializePlan_MultipleFields(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	// Create schema with multiple field types
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddBoolField("active")

	mdm := createTableMetadataWithSchema(t, txn, "multi_field", map[string]interface{}{
		"id":     0,
		"name":   "string",
		"active": true,
	})

	tp, err := NewTablePlan(txn, "multi_field", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	testData := []map[string]interface{}{
		{"id": 1, "name": "Alice", "active": true},
		{"id": 2, "name": "Bob", "active": false},
	}
	insertRecords(t, us, testData)

	mp := NewMaterializePlan(txn, tp)
	matScan, err := mp.Open()
	require.NoError(t, err)
	defer matScan.Close()

	require.NoError(t, matScan.BeforeFirst())
	count := 0
	for {
		hasNext, err := matScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		id, err := matScan.GetInt("id")
		require.NoError(t, err)
		name, err := matScan.GetString("name")
		require.NoError(t, err)
		active, err := matScan.GetBool("active")
		require.NoError(t, err)

		assert.Equal(t, testData[count]["id"], id)
		assert.Equal(t, testData[count]["name"], name)
		assert.Equal(t, testData[count]["active"], active)
		count++
	}
	assert.Equal(t, len(testData), count)
}
