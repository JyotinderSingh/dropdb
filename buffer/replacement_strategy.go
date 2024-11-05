package buffer

// ReplacementStrategy defines the interface for buffer replacement strategies.
type ReplacementStrategy interface {
	// initialize initializes the strategy with the buffer pool.
	initialize(buffers []*Buffer)
	// pinBuffer notifies the strategy that a buffer has been pinned.
	pinBuffer(buff *Buffer)
	// unpinBuffer notifies the strategy that a buffer has been unpinned.
	unpinBuffer(buff *Buffer)
	// chooseUnpinnedBuffer selects an unpinned buffer to replace.
	chooseUnpinnedBuffer() *Buffer
}
