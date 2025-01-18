package plan_impl

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"testing"

	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/scan"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
)

func setupTestEnvironment(t *testing.T, blockSize, numBuffers int) (*tx.Transaction, func()) {
	dbDir := t.TempDir()
	fm, err := file.NewManager(dbDir, blockSize)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, numBuffers)
	lt := concurrency.NewLockTable()
	txn := tx.NewTransaction(fm, lm, bm, lt)

	cleanup := func() {
		require.NoError(t, txn.Commit())
		_ = os.RemoveAll(dbDir)
	}

	return txn, cleanup
}

func createTableMetadataWithSchema(t *testing.T, txn *tx.Transaction, tableName string, schemaFields map[string]interface{}) *metadata.Manager {
	mdm, err := metadata.NewManager(true, txn)
	require.NoError(t, err)

	schema := record.NewSchema()
	for fieldName, fieldType := range schemaFields {
		switch fieldType.(type) {
		case int:
			schema.AddIntField(fieldName)
		case string:
			schema.AddStringField(fieldName, 20)
		case bool:
			schema.AddBoolField(fieldName)
		default:
			t.Fatalf("Unsupported field type for %s: %T", fieldName, fieldType)
		}
	}

	err = mdm.CreateTable(tableName, schema, txn)
	require.NoError(t, err)
	return mdm
}

func insertRecords(t *testing.T, scan scan.UpdateScan, records []map[string]interface{}) {
	for _, record := range records {
		require.NoError(t, scan.Insert())
		for field, value := range record {
			switch v := value.(type) {
			case int:
				require.NoError(t, scan.SetInt(field, v))
			case string:
				require.NoError(t, scan.SetString(field, v))
			case bool:
				require.NoError(t, scan.SetBool(field, v))
			default:
				t.Fatalf("Unsupported value type for %s: %T", field, value)
			}
		}
	}
}

func TestTablePlan_Basic(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "users", map[string]interface{}{
		"id":     0,
		"name":   "string",
		"active": true,
	})

	tp, err := NewTablePlan(txn, "users", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	insertRecords(t, us, []map[string]interface{}{
		{"id": 1, "name": "Alice", "active": true},
		{"id": 2, "name": "Bob", "active": false},
	})

	// Validate records
	s2, err := tp.Open()
	require.NoError(t, err)
	defer s2.Close()

	require.NoError(t, s2.BeforeFirst())
	count := 0
	for {
		hasNext, err := s2.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
		id, err := s2.GetInt("id")
		require.NoError(t, err)
		name, err := s2.GetString("name")
		require.NoError(t, err)
		active, err := s2.GetBool("active")
		require.NoError(t, err)

		assert.Contains(t, []int{1, 2}, id)
		assert.Contains(t, []string{"Alice", "Bob"}, name)
		assert.IsType(t, true, active)
	}
	assert.Equal(t, 2, count)
}

func TestTablePlan_MultiBlock(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "bigTable", map[string]interface{}{
		"num": 0,
		"str": "string",
	})

	tp, err := NewTablePlan(txn, "bigTable", mdm)
	require.NoError(t, err)

	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	r := rand.New(rand.NewSource(1234))
	numRecords := 200
	records := make([]map[string]interface{}, numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = map[string]interface{}{
			"num": r.Intn(5000),
			"str": fmt.Sprintf("val%d", i),
		}
	}
	insertRecords(t, us, records)

	// Validate records
	s2, err := tp.Open()
	require.NoError(t, err)
	defer s2.Close()

	require.NoError(t, s2.BeforeFirst())
	count := 0
	for {
		hasNext, err := s2.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
	}
	assert.Equal(t, numRecords, count)
}

func TestTablePlan_StatsAndSchema(t *testing.T) {
	txn, cleanup := setupTestEnvironment(t, 800, 8)
	defer cleanup()

	mdm := createTableMetadataWithSchema(t, txn, "statsTest", map[string]interface{}{
		"id":   0,
		"name": "string",
	})

	tp, err := NewTablePlan(txn, "statsTest", mdm)
	require.NoError(t, err)

	// Insert records to populate the table
	s, err := tp.Open()
	require.NoError(t, err)
	defer s.Close()

	us, ok := s.(scan.UpdateScan)
	require.True(t, ok)

	records := []map[string]interface{}{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Alice"},
	}
	insertRecords(t, us, records)

	// Re-instantiate the table plan to calculate the updated statistics.
	tp, err = NewTablePlan(txn, "statsTest", mdm)
	require.NoError(t, err)

	// Validate statistics
	assert.Greater(t, tp.BlocksAccessed(), 0, "BlocksAccessed should be greater than 0")
	assert.Equal(t, len(records), tp.RecordsOutput(), "RecordsOutput should match the number of inserted records")
	assert.GreaterOrEqual(t, tp.DistinctValues("id"), 1, "DistinctValues for 'id' should be at least 1")
	assert.GreaterOrEqual(t, tp.DistinctValues("name"), 1, "DistinctValues for 'name' should be at least 1")

	// Validate schema
	schema := tp.Schema()
	require.NotNil(t, schema, "Schema should not be nil")
	assert.True(t, schema.HasField("id"), "Schema should have field 'id'")
	assert.True(t, schema.HasField("name"), "Schema should have field 'name'")
}
