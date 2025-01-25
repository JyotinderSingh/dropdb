package plan_impl

import (
	"testing"

	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/index"
	"github.com/JyotinderSingh/dropdb/index/common"
	"github.com/JyotinderSingh/dropdb/index/hash"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/table"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
)

type indexPlanTestSetup struct {
	transaction *tx.Transaction
	tableScan   *table.Scan
	idx         index.Index
	indexInfo   *metadata.IndexInfo
	cleanup     func()
}

func setupIndexPlanTest(t *testing.T) *indexPlanTestSetup {
	dbDir := t.TempDir()
	fm, err := file.NewManager(dbDir, 800)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "logfile")
	require.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 10)
	transaction := tx.NewTransaction(fm, lm, bm, concurrency.NewLockTable())

	// Create table schema and layout
	tblSchema := record.NewSchema()
	tblSchema.AddIntField("id")
	tblSchema.AddStringField("name", 20)
	tblSchema.AddIntField("val")
	tblLayout := record.NewLayout(tblSchema)

	// Create index schema and layout
	idxSchema := record.NewSchema()
	idxSchema.AddIntField(common.BlockField)
	idxSchema.AddIntField(common.IDField)
	idxSchema.AddIntField(common.DataValueField)
	idxLayout := record.NewLayout(idxSchema)

	// Create table scan
	ts, err := table.NewTableScan(transaction, "test_table", tblLayout)
	require.NoError(t, err)

	// Create index
	idx := hash.NewIndex(transaction, "test_idx", idxLayout)

	// Create StatInfo and IndexInfo
	statInfo := metadata.NewStatInfo(4, 4, map[string]int{
		"id":   4,
		"name": 4,
		"val":  3, // 3 distinct values (10, 20, 40)
	})

	indexInfo := metadata.NewIndexInfo(
		"test_idx",
		"val",
		tblSchema,
		transaction,
		statInfo,
	)

	// Insert test data
	testData := []struct {
		id   int
		name string
		val  int
	}{
		{1, "Alice", 10},
		{2, "Bob", 20},
		{3, "Carol", 20},
		{4, "Dave", 40},
	}

	for _, d := range testData {
		require.NoError(t, ts.Insert())
		require.NoError(t, ts.SetInt("id", d.id))
		require.NoError(t, ts.SetString("name", d.name))
		require.NoError(t, ts.SetInt("val", d.val))

		rid := ts.GetRecordID()
		require.NoError(t, idx.Insert(d.val, rid))
	}

	cleanup := func() {
		ts.Close()
		idx.Close()
		if err := transaction.Commit(); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(dbDir); err != nil {
			t.Error(err)
		}
	}

	return &indexPlanTestSetup{
		transaction: transaction,
		tableScan:   ts,
		idx:         idx,
		indexInfo:   indexInfo,
		cleanup:     cleanup,
	}
}

// Warning: Flaky in group run
func TestIndexSelectPlan_Basic(t *testing.T) {
	setup := setupIndexPlanTest(t)
	defer setup.cleanup()

	mdm := createTableMetadataWithSchema(t, setup.transaction, "test_table", map[string]interface{}{
		"id":   0,
		"name": "string",
		"val":  0,
	})

	tp, err := NewTablePlan(setup.transaction, "test_table", mdm)
	require.NoError(t, err)

	// Create index select plan for val=20
	isp := NewIndexSelectPlan(tp, setup.indexInfo, 20)

	// Test scan execution
	scan, err := isp.Open()
	require.NoError(t, err)
	defer scan.Close()

	matchCount := 0
	require.NoError(t, scan.BeforeFirst())
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		val, err := scan.GetInt("val")
		require.NoError(t, err)
		assert.Equal(t, 20, val)

		name, err := scan.GetString("name")
		require.NoError(t, err)
		assert.Contains(t, []string{"Bob", "Carol"}, name)

		matchCount++
	}
	assert.Equal(t, 2, matchCount)

	// Test plan statistics
	assert.True(t, isp.BlocksAccessed() > 0)
	assert.Equal(t, setup.indexInfo.RecordsOutput(), isp.RecordsOutput())
	assert.Equal(t, 1, isp.DistinctValues("val"))
	assert.Equal(t, 4, isp.DistinctValues("id"))
}

// Warning: Flaky in group run
func TestIndexSelectPlan_NoMatches(t *testing.T) {
	setup := setupIndexPlanTest(t)
	defer setup.cleanup()

	mdm := createTableMetadataWithSchema(t, setup.transaction, "test_table", map[string]interface{}{
		"id":   0,
		"name": "string",
		"val":  0,
	})

	tp, err := NewTablePlan(setup.transaction, "test_table", mdm)
	require.NoError(t, err)

	isp := NewIndexSelectPlan(tp, setup.indexInfo, 99)
	scan, err := isp.Open()
	require.NoError(t, err)
	defer scan.Close()

	require.NoError(t, scan.BeforeFirst())
	hasNext, err := scan.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}

// Warning: Flaky in group run
func TestIndexSelectPlan_SingleMatch(t *testing.T) {
	setup := setupIndexPlanTest(t)
	defer setup.cleanup()

	mdm := createTableMetadataWithSchema(t, setup.transaction, "test_table", map[string]interface{}{
		"id":   0,
		"name": "string",
		"val":  0,
	})

	tp, err := NewTablePlan(setup.transaction, "test_table", mdm)
	require.NoError(t, err)

	isp := NewIndexSelectPlan(tp, setup.indexInfo, 40)
	scan, err := isp.Open()
	require.NoError(t, err)
	defer scan.Close()

	require.NoError(t, scan.BeforeFirst())

	hasNext, err := scan.Next()
	require.NoError(t, err)
	assert.True(t, hasNext)

	name, err := scan.GetString("name")
	require.NoError(t, err)
	assert.Equal(t, "Dave", name)

	val, err := scan.GetInt("val")
	require.NoError(t, err)
	assert.Equal(t, 40, val)

	hasNext, err = scan.Next()
	require.NoError(t, err)
	assert.False(t, hasNext)
}
