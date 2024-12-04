package concurrency

import (
	"context"
	"errors"
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"sync"
	"time"
)

const maxWaitTime = 10 * time.Second

// LockTable provides methods to lock and Unlock blocks.
// If a transaction requests a lock that causes a conflict with an existing lock,
// then that transaction is placed on a wait list.
// There is only one wait list for all blocks.
// When the last lock on a block is unlocked,
// then all transactions are removed from the wait list and rescheduled.
// If one of those transactions discovers that the lock it is waiting for is still locked,
// it will place itself back on the wait list.
type LockTable struct {
	locks map[file.BlockId]int
	mu    sync.Mutex
	cond  *sync.Cond
}

// NewLockTable creates a new LockTable.
func NewLockTable() *LockTable {
	lt := &LockTable{
		locks: make(map[file.BlockId]int),
	}
	lt.cond = sync.NewCond(&lt.mu)
	return lt
}

// SLock grants a shared lock on the specified block.
// If an XLock exists when the method is called,
// then the calling thread will be placed on a wait list
// until the lock is released.
// If the thread remains on the wait list for too long (10 seconds for now),
// then the method will return an error.
func (lt *LockTable) SLock(block *file.BlockId) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
	defer cancel()

	// This function will run after the context expires.
	stop := context.AfterFunc(ctx, func() {
		lt.cond.L.Lock()
		lt.cond.Broadcast()
		lt.cond.L.Unlock()
	})

	defer stop()

	for {
		// If there's no exclusive lock, we can proceed
		if !lt.hasXLock(block) {
			// Get the number of shared locks.
			val := lt.getLockVal(block)
			// Grant the shared lock.
			lt.locks[*block] = val + 1
			return nil
		}

		// Wait until notified or context is done.
		lt.cond.Wait()

		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("lock abort exception: could not acquire shared lock on block %v: %v", block, ctx.Err())
			}
			return ctx.Err()
		}
	}
}

// XLock grants an exclusive lock on the specified block.
// Assumes that the calling thread already has a shared lock on the block.
// If a lock of any type (by some other transaction) exists when the method is called,
// then the calling thread will be placed on a wait list until the locks are released.
// If the thread remains on the wait list for too long (10 seconds for now),
// then the method will return an error.
func (lt *LockTable) XLock(block *file.BlockId) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
	defer cancel()

	stop := context.AfterFunc(ctx, func() {
		lt.cond.L.Lock()
		lt.cond.Broadcast()
		lt.cond.L.Unlock()
	})

	defer stop()

	for {
		// Assume that the calling thread already has a shared lock. If any other shared locks exist, we can't proceed.
		if !lt.hasOtherSLocks(block) {
			lt.locks[*block] = -1
			return nil
		}

		lt.cond.Wait()

		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("lock abort exception: could not acquire exclusive lock on block %v: %v", block, ctx.Err())
			}
			return ctx.Err()
		}
	}
}

// Unlock releases the lock on the specified block.
// If this lock is the last lock on that block,
// then the waiting transactions are notified.
func (lt *LockTable) Unlock(block *file.BlockId) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	val := lt.getLockVal(block)
	if val > 1 {
		lt.locks[*block] = val - 1
	} else {
		delete(lt.locks, *block)
		lt.cond.Broadcast()
	}
}

// hasXLock returns true if there is an exclusive lock on the block.
func (lt *LockTable) hasXLock(block *file.BlockId) bool {
	return lt.getLockVal(block) < 0
}

// hasOtherSLocks returns true if there is more than one shared locks on the block.
func (lt *LockTable) hasOtherSLocks(block *file.BlockId) bool {
	return lt.getLockVal(block) > 1
}

func (lt *LockTable) getLockVal(block *file.BlockId) int {
	return lt.locks[*block]
}
