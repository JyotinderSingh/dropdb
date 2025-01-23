package btree

import (
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/types"
)

// Directory represents a B-tree directory block. It manages the internal nodes
// of the B-tree, handling navigation and structural modifications of the tree.
type Directory struct {
	tx       *tx.Transaction
	layout   *record.Layout
	contents *Page
	filename string
}

// NewDirectory creates a new B-tree directory block manager. It initializes
// the directory with the given block and prepares it for navigation operations.
func NewDirectory(tx *tx.Transaction, blk *file.BlockId, layout *record.Layout) (*Directory, error) {
	contents, err := NewPage(tx, blk, layout)
	if err != nil {
		return nil, err
	}

	return &Directory{
		tx:       tx,
		layout:   layout,
		contents: contents,
		filename: blk.Filename(),
	}, nil
}

// Close releases the resources associated with the directory page.
func (d *Directory) Close() {
	d.contents.Close()
}

// Search traverses the B-tree to find the leaf block containing the specified search key.
// It follows the path from root to leaf, updating its position as it descends the tree.
func (d *Directory) Search(searchKey any) (int, error) {
	// Start by finding the appropriate child block for the search key
	childBlk, err := d.findChildBlock(searchKey)
	if err != nil {
		return -1, err
	}

	// Continue traversing down the tree until we reach a leaf level
	for {
		flag, err := d.contents.GetFlag()
		if err != nil {
			return -1, err
		}
		if flag <= 0 {
			break
		}

		// Move to the next level of the tree
		d.contents.Close()
		contents, err := NewPage(d.tx, childBlk, d.layout)
		if err != nil {
			return -1, err
		}
		d.contents = contents

		childBlk, err = d.findChildBlock(searchKey)
		if err != nil {
			return -1, err
		}
	}

	return childBlk.Number(), nil
}

// MakeNewRoot creates a new root for the B-tree when the current root splits.
// It handles the special case of root splitting by creating a new block for the
// old root's contents and setting up the new root with two children.
func (d *Directory) MakeNewRoot(entry *DirectoryEntry) error {
	// Get the first value from the current root
	firstVal, err := d.contents.GetDataVal(0)
	if err != nil {
		return err
	}

	// Get the current tree level
	level, err := d.contents.GetFlag()
	if err != nil {
		return err
	}

	// Split the current root, moving all its records to a new block
	newBlk, err := d.contents.Split(0, level)
	if err != nil {
		return err
	}

	// Create entries for both the old root contents and the new child
	oldRoot := NewDirectoryEntry(firstVal, newBlk.Number())

	// Insert both entries into the new root
	if _, err := d.insertEntry(oldRoot); err != nil {
		return err
	}
	if _, err := d.insertEntry(entry); err != nil {
		return err
	}

	// Update the root's level
	return d.contents.SetFlag(level + 1)
}

// Insert adds a new entry into the B-tree directory structure. It handles both
// leaf-level and internal node insertions, managing splits when necessary.
func (d *Directory) Insert(entry *DirectoryEntry) (*DirectoryEntry, error) {
	flag, err := d.contents.GetFlag()
	if err != nil {
		return nil, err
	}

	// If we're at the leaf level, insert directly
	if flag == 0 {
		return d.insertEntry(entry)
	}

	// Otherwise, recursively insert into the appropriate child
	childBlk, err := d.findChildBlock(entry.DataValue())
	if err != nil {
		return nil, err
	}

	child, err := NewDirectory(d.tx, childBlk, d.layout)
	if err != nil {
		return nil, err
	}
	defer child.Close()

	// Recursively insert into the child node
	childEntry, err := child.Insert(entry)
	if err != nil {
		return nil, err
	}

	// If child didn't split, we're done
	if childEntry == nil {
		return nil, nil
	}

	// If child split, insert the new entry into this node
	return d.insertEntry(childEntry)
}

// insertEntry adds a directory entry to the current node, handling splits if necessary.
func (d *Directory) insertEntry(entry *DirectoryEntry) (*DirectoryEntry, error) {
	// Find the correct insertion position
	slot, err := d.contents.FindSlotBefore(entry.DataValue())
	if err != nil {
		return nil, err
	}
	newSlot := slot + 1

	// Insert the new entry
	if err := d.contents.InsertDirectory(newSlot, entry.DataValue(), entry.BlockNumber()); err != nil {
		return nil, err
	}

	// Check if we need to split
	isFull, err := d.contents.IsFull()
	if err != nil {
		return nil, err
	}
	if !isFull {
		return nil, nil
	}

	// Handle the split case
	level, err := d.contents.GetFlag()
	if err != nil {
		return nil, err
	}

	numRecs, err := d.contents.GetNumberOfRecords()
	if err != nil {
		return nil, err
	}

	splitPos := numRecs / 2
	splitVal, err := d.contents.GetDataVal(splitPos)
	if err != nil {
		return nil, err
	}

	newBlk, err := d.contents.Split(splitPos, level)
	if err != nil {
		return nil, err
	}

	return NewDirectoryEntry(splitVal, newBlk.Number()), nil
}

// findChildBlock locates the appropriate child block for a given search key.
func (d *Directory) findChildBlock(searchKey any) (*file.BlockId, error) {
	slot, err := d.contents.FindSlotBefore(searchKey)
	if err != nil {
		return nil, err
	}

	// Check if we need to move to the next slot
	nextVal, err := d.contents.GetDataVal(slot + 1)
	if err != nil {
		return nil, err
	}
	if types.CompareSupportedTypes(nextVal, searchKey, types.EQ) {
		slot++
	}

	// Get the child block number
	childNum, err := d.contents.GetChildNumber(slot)
	if err != nil {
		return nil, err
	}

	return file.NewBlockId(d.filename, childNum), nil
}
