package concurrency

import "github.com/JyotinderSingh/dropdb/file"

type Manager struct {
	lockTable *LockTable // pointer to the global lock table.
	locks     map[file.BlockId]string
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{lockTable: NewLockTable(), locks: make(map[file.BlockId]string)}
}

// SLock obtains a shared lock on the block, if necessary.
// The method will ask the lock table for an SLock if the transaction currently has no locks on the block.
func (m *Manager) SLock(block *file.BlockId) error {
	// if the lock doesn't exist in the locks map, acquire it from the lock table.
	if _, ok := m.locks[*block]; !ok {
		if err := m.lockTable.SLock(block); err != nil {
			return err
		}
		m.locks[*block] = "s"
	}
	return nil
}

// XLock obtains an exclusive lock on the block, if necessary.
// If the transaction does not have an exclusive lock on the block,
// the method first gets a shared lock on that block (if necessary), and then upgrades it to an exclusive lock.
func (m *Manager) XLock(block *file.BlockId) error {
	if !m.hasXLock(block) {
		if err := m.SLock(block); err != nil {
			return err
		}
		if err := m.lockTable.XLock(block); err != nil {
			return err
		}
		m.locks[*block] = "x"
	}
	return nil
}

// Release releases all the locks by asking the lock table to Unlock each one.
func (m *Manager) Release() {
	for block := range m.locks {
		m.lockTable.Unlock(&block)
	}
	m.locks = make(map[file.BlockId]string)
}

// hasXLock returns true if the transaction has an exclusive lock on the block.
func (m *Manager) hasXLock(block *file.BlockId) bool {
	lock, ok := m.locks[*block]
	return ok && lock == "x"
}
