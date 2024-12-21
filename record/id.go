package record

import "fmt"

type ID struct {
	blockNumber int
	slot        int
}

// NewID creates a new ID having the specified location in the specified block.
func NewID(blockNumber int, slot int) *ID {
	return &ID{blockNumber, slot}
}

// BlockNumber returns the block number of this ID.
func (id *ID) BlockNumber() int {
	return id.blockNumber
}

// Slot returns the slot number of this ID.
func (id *ID) Slot() int {
	return id.slot
}

func (id *ID) Equals(other *ID) bool {
	return id.blockNumber == other.blockNumber && id.slot == other.slot
}

func (id *ID) String() string {
	return fmt.Sprintf("[%d, %d]", id.blockNumber, id.slot)
}
