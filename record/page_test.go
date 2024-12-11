package record

import (
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func setupTestEnv(t *testing.T) (*tx.Transaction, *file.BlockId, *Layout, func()) {
	// Create temporary directories for testing
	dbDir := t.TempDir()

	// Initialize managers
	fm, err := file.NewManager(dbDir, 400)
	assert.NoError(t, err)

	lm, err := log.NewManager(fm, "test")
	assert.NoError(t, err)

	bm := buffer.NewManager(fm, lm, 10)
	lockTable := concurrency.NewLockTable()

	// Create transaction
	transaction := tx.NewTransaction(fm, lm, bm, lockTable)

	// Create test file and block
	_, err = fm.Append("testfile")
	assert.NoError(t, err)
	blk := file.NewBlockId("testfile", 0)

	// Create test schema and layout
	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	schema.AddBoolField("active")
	schema.AddDateField("created")
	schema.AddLongField("amount")
	schema.AddShortField("type")
	layout := NewLayout(schema)

	cleanup := func() {
		err := transaction.Commit()
		if err != nil {
			t.Fatal(err)
		}
		err = os.RemoveAll(dbDir)
		if err != nil {
			t.Fatal(err)
		}
	}

	return transaction, blk, layout, cleanup
}

func TestNewPage(t *testing.T) {
	transaction, blk, layout, cleanup := setupTestEnv(t)
	defer cleanup()

	page, err := NewPage(transaction, blk, layout)
	assert.NoError(t, err)
	assert.NotNil(t, page)

	// Test Format
	err = page.Format()
	assert.NoError(t, err)
}

func TestPageOperations(t *testing.T) {
	transaction, blk, layout, cleanup := setupTestEnv(t)
	defer cleanup()

	page, err := NewPage(transaction, blk, layout)
	assert.NoError(t, err)

	err = page.Format()
	assert.NoError(t, err)

	// Insert a record
	slot, err := page.InsertAfter(0)
	assert.NoError(t, err)
	assert.True(t, slot > 0)

	t.Run("Integer Operations", func(t *testing.T) {
		err = page.SetInt(slot, "id", 42)
		assert.NoError(t, err)

		val, err := page.GetInt(slot, "id")
		assert.NoError(t, err)
		assert.Equal(t, 42, val)
	})

	t.Run("String Operations", func(t *testing.T) {
		err = page.SetString(slot, "name", "test")
		assert.NoError(t, err)

		val, err := page.GetString(slot, "name")
		assert.NoError(t, err)
		assert.Equal(t, "test", val)
	})

	t.Run("Boolean Operations", func(t *testing.T) {
		err = page.SetBool(slot, "active", true)
		assert.NoError(t, err)

		val, err := page.GetBool(slot, "active")
		assert.NoError(t, err)
		assert.True(t, val)
	})

	t.Run("Date Operations", func(t *testing.T) {
		testTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		err = page.SetDate(slot, "created", testTime)
		assert.NoError(t, err)

		val, err := page.GetDate(slot, "created")
		assert.NoError(t, err)

		// Compare timestamps to handle timezone differences
		assert.Equal(t, testTime.Unix(), val.Unix())
	})

	t.Run("Long Operations", func(t *testing.T) {
		err = page.SetLong(slot, "amount", 9999999999)
		assert.NoError(t, err)

		val, err := page.GetLong(slot, "amount")
		assert.NoError(t, err)
		assert.Equal(t, int64(9999999999), val)
	})

	t.Run("Short Operations", func(t *testing.T) {
		err = page.SetShort(slot, "type", 123)
		assert.NoError(t, err)

		val, err := page.GetShort(slot, "type")
		assert.NoError(t, err)
		assert.Equal(t, int16(123), val)
	})
}

func TestPageSlotManagement(t *testing.T) {
	transaction, blk, layout, cleanup := setupTestEnv(t)
	defer cleanup()

	page, err := NewPage(transaction, blk, layout)
	assert.NoError(t, err)

	err = page.Format()
	assert.NoError(t, err)

	t.Run("Insert and Next After", func(t *testing.T) {
		// Insert first record
		slot1, err := page.InsertAfter(0)
		assert.NoError(t, err)
		assert.True(t, slot1 > 0)

		// Insert second record
		slot2, err := page.InsertAfter(slot1)
		assert.NoError(t, err)
		assert.True(t, slot2 > slot1)

		// Find next used slot
		nextSlot, err := page.NextAfter(slot1)
		assert.NoError(t, err)
		assert.Equal(t, slot2, nextSlot)
	})

	t.Run("Delete and Reuse", func(t *testing.T) {
		// Format page to start fresh
		err = page.Format()
		assert.NoError(t, err)

		// Insert a record
		slot, err := page.InsertAfter(-1)
		assert.NoError(t, err)

		// Set some data
		err = page.SetInt(slot, "id", 42)
		assert.NoError(t, err)

		// Delete the record
		err = page.Delete(slot)
		assert.NoError(t, err)

		// Try to insert after deletion - should get the same slot
		newSlot, err := page.InsertAfter(-1)
		assert.NoError(t, err)
		assert.Equal(t, 0, newSlot)
	})

	t.Run("Slot Capacity", func(t *testing.T) {
		err = page.Format()
		assert.NoError(t, err)

		// Calculate how many slots should fit in a block
		maxSlots := transaction.BlockSize() / layout.SlotSize()

		// Try to insert records until we hit capacity
		var lastSlot int
		var insertErr error
		for i := 0; i < maxSlots+1; i++ {
			lastSlot, insertErr = page.InsertAfter(lastSlot)
			if i >= maxSlots-1 {
				assert.ErrorIs(t, insertErr, ErrNoSlotFound)
				break
			} else {
				assert.NoError(t, insertErr)
			}
		}
	})
}
