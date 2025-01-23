package btree

import (
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/types"
)

// Leaf represents a B-tree leaf block. It encapsulates the functionality
// for managing leaf-level operations in the B-tree structure.
type Leaf struct {
	tx          *tx.Transaction
	layout      *record.Layout
	searchKey   any
	contents    *Page
	currentSlot int
	filename    string
}

// NewLeaf creates and returns a new B-tree leaf page. It positions the cursor
// immediately before the first record that matches the specified search key.
func NewLeaf(tx *tx.Transaction, blk *file.BlockId, layout *record.Layout, searchKey any) (*Leaf, error) {
	contents, err := NewPage(tx, blk, layout)
	if err != nil {
		return nil, err
	}

	// Find the position before the first matching record
	currentSlot, err := contents.FindSlotBefore(searchKey)
	if err != nil {
		contents.Close()
		return nil, err
	}

	return &Leaf{
		tx:          tx,
		layout:      layout,
		searchKey:   searchKey,
		contents:    contents,
		currentSlot: currentSlot,
		filename:    blk.Filename(),
	}, nil
}

// Close releases the resources associated with the leaf page
func (l *Leaf) Close() {
	l.contents.Close()
}

// Next moves to the next leaf record having the previously specified search key.
// Returns false if there are no more matching records.
func (l *Leaf) Next() (bool, error) {
	l.currentSlot++

	numRecs, err := l.contents.GetNumberOfRecords()
	if err != nil {
		return false, err
	}

	if l.currentSlot >= numRecs {
		return l.tryOverflow()
	}

	dataVal, err := l.contents.GetDataVal(l.currentSlot)
	if err != nil {
		return false, err
	}

	if types.CompareSupportedTypes(dataVal, l.searchKey, types.EQ) {
		return true, nil
	}

	return l.tryOverflow()
}

// GetDataRID returns the record ID of the current leaf record
func (l *Leaf) GetDataRID() (*record.ID, error) {
	return l.contents.getDataRID(l.currentSlot)
}

// Delete removes the leaf record with the specified dataRID
func (l *Leaf) Delete(dataRID *record.ID) error {
	for {
		hasNext, err := l.Next()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		currentRID, err := l.GetDataRID()
		if err != nil {
			return err
		}

		if currentRID.Equals(dataRID) {
			return l.contents.delete(l.currentSlot)
		}
	}
	return nil
}

// Insert adds a new leaf record with the specified dataRID and the previously
// specified search key. It returns a directory entry if the page splits.
func (l *Leaf) Insert(dataRID *record.ID) (*DirectoryEntry, error) {
	// Check if we need to handle the special case where the new key
	// should be the first entry
	flag, err := l.contents.GetFlag()
	if err != nil {
		return nil, err
	}

	if flag >= 0 {
		firstVal, err := l.contents.GetDataVal(0)
		if err != nil {
			return nil, err
		}

		if types.CompareSupportedTypes(firstVal, l.searchKey, types.GT) {
			newBlk, err := l.contents.Split(0, flag)
			if err != nil {
				return nil, err
			}

			l.currentSlot = 0
			if err := l.contents.SetFlag(-1); err != nil {
				return nil, err
			}

			if err := l.contents.InsertLeaf(l.currentSlot, l.searchKey, dataRID); err != nil {
				return nil, err
			}

			return NewDirectoryEntry(firstVal, newBlk.Number()), nil
		}
	}

	// Normal insert case
	l.currentSlot++
	if err := l.contents.InsertLeaf(l.currentSlot, l.searchKey, dataRID); err != nil {
		return nil, err
	}

	isFull, err := l.contents.IsFull()
	if err != nil {
		return nil, err
	}

	if !isFull {
		return nil, nil
	}

	// Handle page splitting
	return l.handlePageSplit()
}

// handlePageSplit manages the logic for splitting a full page
func (l *Leaf) handlePageSplit() (*DirectoryEntry, error) {
	firstKey, err := l.contents.GetDataVal(0)
	if err != nil {
		return nil, err
	}

	numRecs, err := l.contents.GetNumberOfRecords()
	if err != nil {
		return nil, err
	}

	lastKey, err := l.contents.GetDataVal(numRecs - 1)
	if err != nil {
		return nil, err
	}

	// Handle the case where all keys are the same
	if types.CompareSupportedTypes(lastKey, firstKey, types.EQ) {
		flag, err := l.contents.GetFlag()
		if err != nil {
			return nil, err
		}
		newBlk, err := l.contents.Split(1, flag)
		if err != nil {
			return nil, err
		}

		if err := l.contents.SetFlag(newBlk.Number()); err != nil {
			return nil, err
		}

		return nil, nil
	}

	// Normal split case
	splitPos := numRecs / 2
	splitKey, err := l.contents.GetDataVal(splitPos)
	if err != nil {
		return nil, err
	}

	// Adjust split position based on key distribution
	if types.CompareSupportedTypes(splitKey, firstKey, types.EQ) {
		for {
			val, err := l.contents.GetDataVal(splitPos)
			if err != nil {
				return nil, err
			}
			if !types.CompareSupportedTypes(val, splitKey, types.EQ) {
				break
			}
			splitPos++
			splitKey = val
		}
	} else {
		for splitPos > 0 {
			val, err := l.contents.GetDataVal(splitPos - 1)
			if err != nil {
				return nil, err
			}
			if !types.CompareSupportedTypes(val, splitKey, types.EQ) {
				break
			}
			splitPos--
		}
	}

	newBlk, err := l.contents.Split(splitPos, -1)
	if err != nil {
		return nil, err
	}

	return NewDirectoryEntry(splitKey, newBlk.Number()), nil
}

// tryOverflow attempts to follow the overflow chain for matching records
func (l *Leaf) tryOverflow() (bool, error) {
	firstKey, err := l.contents.GetDataVal(0)
	if err != nil {
		return false, err
	}

	flag, err := l.contents.GetFlag()
	if err != nil {
		return false, err
	}

	if !types.CompareSupportedTypes(l.searchKey, firstKey, types.EQ) || flag < 0 {
		return false, nil
	}

	l.contents.Close()
	nextBlk := file.NewBlockId(l.filename, flag)
	contents, err := NewPage(l.tx, nextBlk, l.layout)
	if err != nil {
		return false, err
	}

	l.contents = contents
	l.currentSlot = 0
	return true, nil
}
