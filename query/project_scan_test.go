package query

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestProjectScan_Basic projects only a subset of fields: ["id", "name"].
func TestProjectScan_Basic(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// We will only project "id" and "name".
	fieldList := []string{"id", "name"}
	ps, err := NewProjectScan(ts, fieldList)
	require.NoError(t, err, "failed to create ProjectScan")
	defer ps.Close()

	// Expect to see 4 records, each with "id" and "name" accessible.
	count := 0
	require.NoError(t, ps.BeforeFirst())
	for {
		hasNext, err := ps.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		// Access "id" and "name"
		_, err = ps.GetInt("id")
		require.NoError(t, err)

		_, err = ps.GetString("name")
		require.NoError(t, err)

		// This field is NOT in the projection → expect error
		_, err = ps.GetInt("val")
		assert.Error(t, err, "expected error for missing field 'val'")
		assert.ErrorContainsf(t, err, fmt.Sprintf(ErrFieldNotFound, "val"), "expected error for missing field 'val'")

		count++
	}
	assert.Equal(t, 4, count, "expected 4 projected rows")
}

// TestProjectScan_FieldNotFound ensures calls to a missing field produce an error.
func TestProjectScan_FieldNotFound(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// Project *only* the field "id"
	ps, err := NewProjectScan(ts, []string{"id"})
	require.NoError(t, err)
	defer ps.Close()

	require.NoError(t, ps.BeforeFirst())
	hasNext, err := ps.Next()
	require.NoError(t, err)
	require.True(t, hasNext, "expected at least one record")

	// "id" is projected → should succeed
	_, err = ps.GetInt("id")
	require.NoError(t, err)

	// "name" not in projection → expect error
	_, err = ps.GetString("name")
	assert.Error(t, err, "expected error for missing field 'name'")
	assert.ErrorContainsf(t, err, fmt.Sprintf(ErrFieldNotFound, "name"), "expected error for missing field 'name'")
}

// TestProjectScan_Update tests set operations on projected fields.
func TestProjectScan_Update(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// Project "id" and "val". We *omitted* "name" to illustrate that we can’t update it.
	fields := []string{"id", "val"}
	ps, err := NewProjectScan(ts, fields)
	require.NoError(t, err)
	defer ps.Close()

	require.NoError(t, ps.BeforeFirst())

	updatedCount := 0
	for {
		hasNext, err := ps.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		// We *can* read "val"
		oldVal, err := ps.GetInt("val")
		require.NoError(t, err)

		// We *can* update "val"
		err = ps.SetInt("val", oldVal+100)
		require.NoError(t, err)

		updatedCount++
	}
	assert.Equal(t, 4, updatedCount, "Should have updated 4 records in 'val'")

	// Now verify underlying TableScan has these changes for 'val'
	require.NoError(t, ts.BeforeFirst())
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		// The original data: 10, 20, 30, 40 → now should be 110, 120, 130, 140
		val, err := ts.GetInt("val")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, val, 110)
	}
}

// TestProjectScan_UpdateMissingField ensures we get an error if we try to update
// a field not in the projection.
func TestProjectScan_UpdateMissingField(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// Project *only* "id"
	ps, err := NewProjectScan(ts, []string{"id"})
	require.NoError(t, err)
	defer ps.Close()

	require.NoError(t, ps.BeforeFirst())
	hasNext, err := ps.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	// "val" is not in the projection → set attempt should fail
	err = ps.SetInt("val", 999)
	assert.Error(t, err, "expected error for updating a missing field 'val'")
	assert.ErrorContainsf(t, err, fmt.Sprintf(ErrFieldNotFound, "val"), "expected error for updating a missing field 'val'")
}

// TestProjectScan_Delete tests delete operations if underlying scan is an UpdateScan.
func TestProjectScan_Delete(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// We project "id" only. That means we can’t see "val" or "name", but
	// we can still delete from the underlying scan if it's updatable.
	ps, err := NewProjectScan(ts, []string{"id"})
	require.NoError(t, err)
	defer ps.Close()

	require.NoError(t, ps.BeforeFirst())

	deletedCount := 0
	for {
		hasNext, err := ps.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		// We'll delete every row for demonstration
		err = ps.Delete()
		require.NoError(t, err)
		deletedCount++
	}
	assert.Equal(t, 4, deletedCount, "Should have deleted all 4 rows")

	// Now the underlying TableScan should have no rows
	require.NoError(t, ts.BeforeFirst())
	hasNext, err := ts.Next()
	require.NoError(t, err)
	assert.False(t, hasNext, "Expected no rows left after deleting all")
}

// TestProjectScan_Insert to illustrate inserting new records through a ProjectScan.
func TestProjectScan_Insert(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// We'll project "id" and "name" only.
	ps, err := NewProjectScan(ts, []string{"id", "name"})
	require.NoError(t, err)
	defer ps.Close()

	// Insert a new record
	err = ps.Insert()
	require.NoError(t, err)

	// Set projected fields
	require.NoError(t, ps.SetInt("id", 999))
	require.NoError(t, ps.SetString("name", "Zack"))

	// Attempting to set non-projected field "val" → error
	err = ps.SetInt("val", 9999)
	assert.Error(t, err, "Can't set non-projected field val")

	// Now verify the inserted row is in underlying scan
	require.NoError(t, ts.BeforeFirst())
	foundNewRecord := false
	for {
		hasNext, err := ts.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		idVal, err := ts.GetInt("id")
		require.NoError(t, err)
		if idVal == 999 {
			foundNewRecord = true
			nameVal, err := ts.GetString("name")
			require.NoError(t, err)
			assert.Equal(t, "Zack", nameVal, "Inserted record's 'name' should be Zack")
		}
	}
	assert.True(t, foundNewRecord, "Should have found the newly inserted record (id=999)")
}

// TestProjectScan_GetRecordID ensures we can retrieve & move by RID if underlying scan allows it.
func TestProjectScan_GetRecordID(t *testing.T) {
	ts, cleanup := setupTestTableScan(t)
	defer cleanup()

	// Project just "id".
	ps, err := NewProjectScan(ts, []string{"id"})
	require.NoError(t, err)
	defer ps.Close()

	require.NoError(t, ps.BeforeFirst())

	// We'll find Bob (id=2), store his record ID.
	var bobRID *record.ID
	for {
		hasNext, err := ps.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		idVal, err := ps.GetInt("id")
		require.NoError(t, err)
		if idVal == 2 {
			bobRID = ps.GetRecordID()
			break
		}
	}
	require.NotNil(t, bobRID, "Should have found Bob with id=2")

	// Now move to record ID for Bob
	err = ps.MoveToRecordID(bobRID)
	require.NoError(t, err, "MoveToRecordID should succeed if underlying scan supports it")

	// Verify we're indeed at Bob
	idVal, err := ps.GetInt("id")
	require.NoError(t, err)
	assert.Equal(t, 2, idVal, "After MoveToRecordID, we should see Bob's id=2")
}
