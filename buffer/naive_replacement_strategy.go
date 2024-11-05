package buffer

import "sync"

// NaiveStrategy is a simple buffer replacement strategy that selects the first unpinned buffer.
type NaiveStrategy struct {
	ReplacementStrategy
	buffers []*Buffer
	mu      sync.Mutex
}

// NewNaiveStrategy creates a new NaiveStrategy.
func NewNaiveStrategy() *NaiveStrategy {
	return &NaiveStrategy{}
}

// Initialize initializes the strategy with the buffer pool.
func (ns *NaiveStrategy) initialize(buffers []*Buffer) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.buffers = buffers
}

// PinBuffer notifies the strategy that a buffer has been pinned.
// No action needed for naive strategy.
func (ns *NaiveStrategy) pinBuffer(buff *Buffer) {
	// No action needed.
}

// UnpinBuffer notifies the strategy that a buffer has been unpinned.
// No action needed for naive strategy.
func (ns *NaiveStrategy) unpinBuffer(buff *Buffer) {
	// No action needed.
}

// ChooseUnpinnedBuffer selects an unpinned buffer to replace.
func (ns *NaiveStrategy) chooseUnpinnedBuffer() *Buffer {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	for _, buff := range ns.buffers {
		if !buff.isPinned() {
			return buff
		}
	}
	return nil
}
