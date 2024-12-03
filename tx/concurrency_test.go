package tx

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
)

// TestConcurrency runs the concurrency test using testify assertions.
func TestConcurrency(t *testing.T) {
	// Initialize the database system
	fm, err := file.NewManager("concurrencytest", 400)
	assert.NoError(t, err, "Error initializing file manager")

	lm, _ := log.NewManager(fm, "logfile")
	bm := buffer.NewManager(fm, lm, 8) // 8 buffers

	// Initialize blocks to ensure "testfile" has blocks 0, 1, and 2
	err = initializeBlocks(fm, lm, bm)
	assert.NoError(t, err, "Error initializing blocks")

	var wg sync.WaitGroup
	wg.Add(3) // 3 transactions

	// Use channels to capture errors from goroutines
	errCh := make(chan error, 3)

	// Start transactions A, B, and C in separate goroutines
	go func() {
		defer wg.Done()
		err := transactionA(fm, lm, bm)
		errCh <- err
	}()
	go func() {
		defer wg.Done()
		err := transactionB(fm, lm, bm)
		errCh <- err
	}()
	go func() {
		defer wg.Done()
		err := transactionC(fm, lm, bm)
		errCh <- err
	}()

	wg.Wait()
	close(errCh)

	// Check for errors from transactions
	for err := range errCh {
		assert.NoError(t, err)
	}
}

// initializeBlocks ensures that "testfile" has at least 3 blocks (0, 1, 2)
func initializeBlocks(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) error {
	txInit := NewTransaction(fm, lm, bm)
	defer func() {
		err := txInit.Commit()
		if err != nil {
			// Since we're in a helper function, we print the error
			// The main test function will handle any errors returned
			println("Error committing initialization transaction:", err.Error())
		}
	}()

	size, err := txInit.Size("testfile")
	if err != nil {
		return err
	}
	for i := size; i <= 2; i++ {
		_, err := txInit.Append("testfile")
		if err != nil {
			return err
		}
		// Optionally, initialize block contents here
	}
	return nil
}

// transactionA corresponds to Transaction A in the original Java code
func transactionA(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) error {
	txA := NewTransaction(fm, lm, bm)
	blk1 := file.NewBlockId("testfile", 1)
	blk2 := file.NewBlockId("testfile", 2)

	err := txA.Pin(blk1)
	if err != nil {
		return err
	}
	err = txA.Pin(blk2)
	if err != nil {
		return err
	}

	println("Tx A: request slock 1")
	_, err = txA.GetInt(blk1, 0)
	if err != nil {
		return err
	}
	println("Tx A: receive slock 1")
	time.Sleep(1 * time.Second)
	println("Tx A: request slock 2")
	_, err = txA.GetInt(blk2, 0)
	if err != nil {
		return err
	}
	println("Tx A: receive slock 2")
	err = txA.Commit()
	if err != nil {
		return err
	}
	println("Tx A: commit")
	return nil
}

// transactionB corresponds to Transaction B in the original Java code
func transactionB(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) error {
	txB := NewTransaction(fm, lm, bm)
	blk1 := file.NewBlockId("testfile", 1)
	blk2 := file.NewBlockId("testfile", 2)

	err := txB.Pin(blk1)
	if err != nil {
		return err
	}
	err = txB.Pin(blk2)
	if err != nil {
		return err
	}

	println("Tx B: request xlock 2")
	err = txB.SetInt(blk2, 0, 0, false)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			println("Tx B: lock abort exception on block 2:", err.Error())
			_ = txB.Rollback()
			return err
		}
		return err
	}
	println("Tx B: receive xlock 2")
	time.Sleep(1 * time.Second)
	println("Tx B: request slock 1")
	_, err = txB.GetInt(blk1, 0)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			println("Tx B: lock abort exception on block 1:", err.Error())
			_ = txB.Rollback()
			return err
		}
		return err
	}
	println("Tx B: receive slock 1")
	err = txB.Commit()
	if err != nil {
		return err
	}
	println("Tx B: commit")
	return nil
}

// transactionC corresponds to Transaction C in the original Java code
func transactionC(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) error {
	txC := NewTransaction(fm, lm, bm)
	blk1 := file.NewBlockId("testfile", 1)
	blk2 := file.NewBlockId("testfile", 2)

	err := txC.Pin(blk1)
	if err != nil {
		return err
	}
	err = txC.Pin(blk2)
	if err != nil {
		return err
	}

	time.Sleep(500 * time.Millisecond)
	println("Tx C: request xlock 1")
	err = txC.SetInt(blk1, 0, 0, false)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			println("Tx C: lock abort exception on block 1:", err.Error())
			_ = txC.Rollback()
			return err
		}
		return err
	}
	println("Tx C: receive xlock 1")
	time.Sleep(1 * time.Second)
	println("Tx C: request slock 2")
	_, err = txC.GetInt(blk2, 0)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			println("Tx C: lock abort exception on block 2:", err.Error())
			_ = txC.Rollback()
			return err
		}
		return err
	}
	println("Tx C: receive slock 2")
	err = txC.Commit()
	if err != nil {
		return err
	}
	println("Tx C: commit")
	return nil
}
