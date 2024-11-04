package buffer

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testEnv encapsulates the test environment
type testEnv struct {
	fm      *file.Manager
	lm      *log.Manager
	bm      *Manager
	cleanup func()
}

// setupTest creates a new test environment with the specified number of buffers
func setupTest(t *testing.T, numBuffers int) *testEnv {
	t.Helper()
	dbDir := filepath.Join(os.TempDir(), "testdb")
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	fm, err := file.NewManager(dbDir, 400)
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "testlog")
	require.NoError(t, err)

	cleanup := func() {
		_ = os.RemoveAll(dbDir)
	}

	return &testEnv{
		fm:      fm,
		lm:      lm,
		bm:      NewManager(fm, lm, numBuffers),
		cleanup: cleanup,
	}
}

// createBlock is a helper to create a block ID
func createBlock(fileName string, blockNum int) file.BlockId {
	return file.BlockId{File: fileName, BlockNumber: blockNum}
}

func TestBufferManager(t *testing.T) {
	t.Run("basic buffer operations", func(t *testing.T) {
		env := setupTest(t, 3)
		defer env.cleanup()

		// Test single pin/unpin
		blk := createBlock("testfile", 1)
		buff, err := env.bm.Pin(&blk)
		require.NoError(t, err)
		assert.Equal(t, &blk, buff.Block(), "buffer should be assigned to correct block")

		env.bm.Unpin(buff)
		assert.Equal(t, 3, env.bm.Available(), "buffer should be available after unpinning")
	})

	t.Run("buffer allocation until full", func(t *testing.T) {
		env := setupTest(t, 3)
		defer env.cleanup()

		// Pin three blocks
		blocks := make([]*Buffer, 3)
		for i := 0; i < 3; i++ {
			blk := createBlock("testfile", i+1)
			buff, err := env.bm.Pin(&blk)
			require.NoError(t, err)
			assert.Equal(t, &blk, buff.Block())
			blocks[i] = buff
		}

		assert.Equal(t, 0, env.bm.Available(), "no buffers should be available")

		// Cleanup
		for _, buff := range blocks {
			env.bm.Unpin(buff)
		}
	})

	t.Run("buffer reuse after unpin", func(t *testing.T) {
		env := setupTest(t, 2)
		defer env.cleanup()

		// Pin first block
		blk1 := createBlock("testfile", 1)
		buff1, err := env.bm.Pin(&blk1)
		require.NoError(t, err)

		// Pin second block
		blk2 := createBlock("testfile", 2)
		_, err = env.bm.Pin(&blk2)
		require.NoError(t, err)

		// Unpin first buffer
		env.bm.Unpin(buff1)

		// Pin third block, should reuse first buffer
		blk3 := createBlock("testfile", 3)
		buff3, err := env.bm.Pin(&blk3)
		require.NoError(t, err)
		assert.Equal(t, buff1, buff3, "should reuse unpinned buffer")
	})
}

func TestBufferTimeout(t *testing.T) {
	env := setupTest(t, 1)
	defer env.cleanup()

	// Pin the only available buffer
	blk1 := createBlock("testfile", 1)
	buff1, err := env.bm.Pin(&blk1)
	require.NoError(t, err)

	// Try to pin another block, should timeout
	done := make(chan error, 1)
	go func() {
		blk2 := createBlock("testfile", 2)
		_, err := env.bm.Pin(&blk2)
		done <- err
	}()

	select {
	case err := <-done:
		assert.ErrorContains(t, err, "buffer abort exception")
		assert.ErrorContains(t, err, "context deadline exceeded")
	case <-time.After(12 * time.Second):
		t.Fatal("timeout waiting for Pin to return error")
	}

	env.bm.Unpin(buff1)

	// Try to pin blk2 again - should succeed now
	blk2 := createBlock("testfile", 2)
	buff2, err := env.bm.Pin(&blk2)
	require.NoError(t, err, "should successfully pin block after buffer becomes available")
	assert.Equal(t, &blk2, buff2.Block(), "buffer should be assigned to correct block")

	// Cleanup
	env.bm.Unpin(buff2)
}

func TestConcurrentBufferAccess(t *testing.T) {
	env := setupTest(t, 2)
	defer env.cleanup()

	var wg sync.WaitGroup
	workDuration := 3 * time.Second

	// Pin first block
	wg.Add(1)
	go func() {
		defer wg.Done()
		blk1 := createBlock("testfile", 1)
		buff1, err := env.bm.Pin(&blk1)
		require.NoError(t, err)
		time.Sleep(workDuration) // Simulate work
		env.bm.Unpin(buff1)
	}()

	// Pin second block
	wg.Add(1)
	go func() {
		defer wg.Done()
		blk2 := createBlock("testfile", 2)
		buff2, err := env.bm.Pin(&blk2)
		require.NoError(t, err)
		time.Sleep(workDuration) // Simulate work
		env.bm.Unpin(buff2)
	}()

	// Wait a bit to ensure both blocks are pinned
	time.Sleep(50 * time.Millisecond)

	// Try to pin third block - should wait until one of the buffers becomes available
	start := time.Now()
	blk3 := createBlock("testfile", 3)
	buff3, err := env.bm.Pin(&blk3)
	require.NoError(t, err)

	waitDuration := time.Since(start)
	assert.GreaterOrEqual(t,
		waitDuration.Seconds(),
		2.9,
		"Expected to wait at least 3 seconds before pinning third block, but waited %v",
		waitDuration,
	)

	env.bm.Unpin(buff3)
	wg.Wait()

	assert.Equal(t, 2, env.bm.Available(), "all buffers should be available after completion")
}
