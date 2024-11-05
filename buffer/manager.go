package buffer

import (
	"context"
	"errors"
	"fmt"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"sync"
	"time"
)

// maxWaitTime is the maximum time to wait for a buffer to become available.
const maxWaitTime = 10 * time.Second

// Manager manages the pinning and unpinning of buffers to blocks.
type Manager struct {
	bufferPool   []*Buffer
	numAvailable int
	mu           sync.Mutex
	cond         *sync.Cond
	strategy     ReplacementStrategy
}

// NewManager creates a buffer manager having the specified number of buffer slots.
// It depends on a file.Manager and log.Manager instance. Uses the Naive replacement strategy by default.
func NewManager(fileManager *file.Manager, logManager *log.Manager, numBuffers int) *Manager {
	return NewManagerWithReplacementStrategy(fileManager, logManager, numBuffers, NewNaiveStrategy())
}

// NewManagerWithReplacementStrategy creates a buffer manager with a given replacement strategy having the specified number of buffer slots.
// It depends on a file.Manager and log.Manager instance.
func NewManagerWithReplacementStrategy(fileManager *file.Manager, logManager *log.Manager, numBuffers int, strategy ReplacementStrategy) *Manager {
	bm := &Manager{
		bufferPool:   make([]*Buffer, numBuffers),
		numAvailable: numBuffers,
		strategy:     strategy,
	}
	bm.cond = sync.NewCond(&bm.mu)
	for i := 0; i < numBuffers; i++ {
		bm.bufferPool[i] = NewBuffer(fileManager, logManager)
	}
	// initialize the strategy with the buffer pool
	strategy.initialize(bm.bufferPool)
	return bm
}

// Available returns the number of available (i.e., unpinned) buffers.
func (m *Manager) Available() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.numAvailable
}

// FlushAll flushes the dirty buffers modified by the specified transaction.
func (m *Manager) FlushAll(txnNum int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, buff := range m.bufferPool {
		if buff.modifyingTxn() == txnNum {
			if err := buff.flush(); err != nil {
				return fmt.Errorf("failed to flush buffer for txn %d: %v", txnNum, err)
			}
		}
	}
	return nil
}

// Unpin unpins the specified buffer. If its pin count goes to zero, it increases the number
// of available buffers and notifies any waiting goroutines.
func (m *Manager) Unpin(buffer *Buffer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	buffer.unpin()
	m.strategy.unpinBuffer(buffer)
	if !buffer.isPinned() {
		m.numAvailable++
		m.cond.Broadcast()
	}
}

// Pin pins a buffer to the specified block, potentially waiting until a buffer becomes available.
// If no buffer becomes available within a fixed time period, it returns an error.
// This function uses conditional with wait pattern, it can be found detailed here: https://pkg.go.dev/context#example-AfterFunc-Cond
func (m *Manager) Pin(block *file.BlockId) (*Buffer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
	defer cancel()

	var buff *Buffer
	var err error

	waitOnCond := func() error {
		// Set up a goroutine to cancel the wait when the context is done.
		done := make(chan struct{})
		defer close(done)

		go func() {
			select {
			case <-ctx.Done():
				m.mu.Lock()
				// Wake up the conditional.
				m.cond.Broadcast()
				m.mu.Unlock()
			case <-done:
				// The condition was met before the context was canceled.
			}
		}()

		for {
			if buff, err = m.tryToPin(block); err != nil {
				return err
			}
			if buff != nil {
				break
			}
			m.cond.Wait()

			// Check if the context has errored out (due to timeout).
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		return nil
	}

	if err := waitOnCond(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("buffer abort exception: could not pin block %s: %v", block.String(), err)
		}
		return nil, err
	}
	return buff, nil
}

// tryToPin tries to pin a buffer to the specified block.
// If there is already a buffer assigned to that block, it uses that buffer.
// Otherwise, it chooses an unpinned buffer from the pool.
// Returns nil if there are no available buffers.
// This method is not thread-safe.
func (m *Manager) tryToPin(block *file.BlockId) (*Buffer, error) {
	buffer := m.findExistingBuffer(block)
	if buffer == nil {
		buffer = m.strategy.chooseUnpinnedBuffer()
		if buffer == nil {
			return nil, nil
		}
		if err := buffer.assignToBlock(block); err != nil {
			return nil, err
		}
	}
	if !buffer.isPinned() {
		m.numAvailable--
	}
	buffer.pin()
	m.strategy.pinBuffer(buffer)
	return buffer, nil
}

// findExistingBuffer searches for a buffer assigned to the specified block.
func (m *Manager) findExistingBuffer(block *file.BlockId) *Buffer {
	for _, buffer := range m.bufferPool {
		b := buffer.Block()
		if b != nil && b.Equals(block) {
			return buffer
		}
	}
	return nil
}
