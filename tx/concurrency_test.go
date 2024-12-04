package tx_test

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/tx"
)

type TransactionResult struct {
	Name      string
	Committed bool
	Aborted   bool
	Error     error
	TxNum     int
}

func TestConcurrencySuccess(t *testing.T) {
	dir := fmt.Sprintf("testdir_%d", rand.Int())
	// Initialize the database system
	fm, err := file.NewManager(dir, 400)
	assert.NoError(t, err, "Error initializing file manager")
	// Delete the db directory and all its contents after the test
	defer func() {
		err := os.RemoveAll(dir)
		if err != nil {
			return
		}
	}()

	lm, err := log.NewManager(fm, "logfile")
	assert.NoError(t, err, "Error initializing log manager")
	bm := buffer.NewManager(fm, lm, 8) // 8 buffers
	lt := concurrency.NewLockTable()

	assert.NoError(t, err, "Error initializing blocks")

	var wg sync.WaitGroup
	wg.Add(3) // 3 transactions

	// Use channels to capture results from goroutines
	resultCh := make(chan *TransactionResult, 3)

	// Start transactions A, B, and C in separate goroutines
	go func() {
		defer wg.Done()
		result := transactionA(fm, lm, bm, lt)
		resultCh <- result
	}()
	go func() {
		defer wg.Done()
		result := transactionB(fm, lm, bm, lt)
		resultCh <- result
	}()
	go func() {
		defer wg.Done()
		result := transactionC(fm, lm, bm, lt)
		resultCh <- result
	}()

	wg.Wait()
	close(resultCh)

	// Collect results
	results := make(map[string]*TransactionResult)
	for result := range resultCh {
		results[result.Name] = result
	}

	// Assertions
	assert.Equal(t, 3, len(results), "Expected results from 3 transactions")

	// All transactions should have committed successfully and have valid transaction numbers
	usedTxNums := make(map[int]bool)
	var lastTxNum int
	for name, result := range results {
		assert.NotNil(t, result, "Transaction %s result missing", name)
		assert.True(t, result.Committed, "Transaction %s should have committed", name)
		assert.False(t, result.Aborted, "Transaction %s should not have aborted", name)
		assert.NoError(t, result.Error, "Transaction %s should not have error", name)

		assert.True(t, result.TxNum >= 1 && result.TxNum <= 3, "Transaction %s number should be between 1 and 3, got %d", name, result.TxNum)
		assert.False(t, usedTxNums[result.TxNum], "Transaction number %d was used more than once", result.TxNum)
		if lastTxNum > 0 {
			assert.NotEqual(t, lastTxNum, result.TxNum, "Transaction number %d was repeated", result.TxNum)
		}
		lastTxNum = result.TxNum
		usedTxNums[result.TxNum] = true
	}
	assert.Equal(t, 3, len(usedTxNums), "Should have exactly 3 different transaction numbers")
}

// Transaction A reads blocks 1 and 2, then commits
func transactionA(fm *file.Manager, lm *log.Manager, bm *buffer.Manager, lt *concurrency.LockTable) *TransactionResult {
	result := &TransactionResult{Name: "A"}

	txA := tx.NewTransaction(fm, lm, bm, lt)
	result.TxNum = txA.TxNum()

	blk1 := file.NewBlockId("testfile", 1)
	blk2 := file.NewBlockId("testfile", 2)

	err := txA.Pin(blk1)
	if err != nil {
		result.Error = err
		return result
	}
	err = txA.Pin(blk2)
	if err != nil {
		result.Error = err
		return result
	}

	_, err = txA.GetInt(blk1, 0)
	if err != nil {
		result.Error = err
		return result
	}
	time.Sleep(1 * time.Second)
	_, err = txA.GetInt(blk2, 0)
	if err != nil {
		result.Error = err
		return result
	}
	err = txA.Commit()
	if err != nil {
		result.Error = err
		return result
	}
	result.Committed = true
	return result
}

// Transaction B reads block 1 and writes block 2, then commits
func transactionB(fm *file.Manager, lm *log.Manager, bm *buffer.Manager, lt *concurrency.LockTable) *TransactionResult {
	result := &TransactionResult{Name: "B"}

	txB := tx.NewTransaction(fm, lm, bm, lt)
	result.TxNum = txB.TxNum()

	blk1 := file.NewBlockId("testfile", 1)
	blk2 := file.NewBlockId("testfile", 2)

	err := txB.Pin(blk1)
	if err != nil {
		result.Error = err
		return result
	}
	err = txB.Pin(blk2)
	if err != nil {
		result.Error = err
		return result
	}

	err = txB.SetInt(blk2, 0, 0, false)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			_ = txB.Rollback()
			result.Error = err
			result.Aborted = true
			return result
		}
		result.Error = err
		return result
	}
	time.Sleep(1 * time.Second)
	_, err = txB.GetInt(blk1, 0)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			_ = txB.Rollback()
			result.Error = err
			result.Aborted = true
			return result
		}
		result.Error = err
		return result
	}
	err = txB.Commit()
	if err != nil {
		result.Error = err
		return result
	}
	result.Committed = true
	return result
}

// Transaction C writes block 1 and reads block 2, then commits
func transactionC(fm *file.Manager, lm *log.Manager, bm *buffer.Manager, lt *concurrency.LockTable) *TransactionResult {
	result := &TransactionResult{Name: "C"}

	txC := tx.NewTransaction(fm, lm, bm, lt)
	result.TxNum = txC.TxNum()

	blk1 := file.NewBlockId("testfile", 1)
	blk2 := file.NewBlockId("testfile", 2)

	err := txC.Pin(blk1)
	if err != nil {
		result.Error = err
		return result
	}
	err = txC.Pin(blk2)
	if err != nil {
		result.Error = err
		return result
	}

	time.Sleep(500 * time.Millisecond)
	err = txC.SetInt(blk1, 0, 0, false)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			_ = txC.Rollback()
			result.Error = err
			result.Aborted = true
			return result
		}
		result.Error = err
		return result
	}
	time.Sleep(1 * time.Second)
	_, err = txC.GetInt(blk2, 0)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			_ = txC.Rollback()
			result.Error = err
			result.Aborted = true
			return result
		}
		result.Error = err
		return result
	}
	err = txC.Commit()
	if err != nil {
		result.Error = err
		return result
	}
	result.Committed = true
	return result
}

func TestDeadlockDetection(t *testing.T) {
	dir := fmt.Sprintf("testdir_%d", rand.Int())
	// Initialize the database system
	fm, err := file.NewManager(dir, 400)
	assert.NoError(t, err, "Error initializing file manager")
	// Delete the db directory and all its contents after the test
	defer func() {
		err := os.RemoveAll(dir)
		if err != nil {
			return
		}
	}()

	lm, err := log.NewManager(fm, "logfile")
	assert.NoError(t, err, "Error initializing log manager")
	bm := buffer.NewManager(fm, lm, 8) // 8 buffers
	lt := concurrency.NewLockTable()

	var wg sync.WaitGroup
	wg.Add(2) // 2 transactions

	// Use channels to capture results from goroutines
	resultCh := make(chan *TransactionResult, 2)

	// Start transactions A and B in separate goroutines
	go func() {
		defer wg.Done()
		result := transactionDeadlockA(fm, lm, bm, lt)
		resultCh <- result
	}()
	go func() {
		defer wg.Done()
		result := transactionDeadlockB(fm, lm, bm, lt)
		resultCh <- result
	}()

	wg.Wait()
	close(resultCh)

	// Collect results
	results := make(map[string]*TransactionResult)
	for result := range resultCh {
		results[result.Name] = result
	}

	// Assertions
	assert.Equal(t, 2, len(results), "Expected results from 2 transactions")

	// One transaction should have committed, and the other should have aborted due to deadlock
	resultA := results["A"]
	resultB := results["B"]
	assert.NotNil(t, resultA, "Transaction A result missing")
	assert.NotNil(t, resultB, "Transaction B result missing")

	for _, result := range []*TransactionResult{resultA, resultB} {
		assert.True(t, result.Aborted, "Transaction should have aborted")
		assert.Error(t, result.Error, "Aborted transaction should have error")
		assert.Contains(t, result.Error.Error(), "lock abort exception", "Aborted transaction should have lock abort error")
	}

}

// transactionDeadlockA tries to write to block 1 and then block 2
func transactionDeadlockA(fm *file.Manager, lm *log.Manager, bm *buffer.Manager, lt *concurrency.LockTable) *TransactionResult {
	result := &TransactionResult{Name: "A"}

	txA := tx.NewTransaction(fm, lm, bm, lt)
	result.TxNum = txA.TxNum()

	blk1 := file.NewBlockId("testfile", 1)
	blk2 := file.NewBlockId("testfile", 2)

	err := txA.Pin(blk1)
	if err != nil {
		result.Error = err
		return result
	}
	err = txA.Pin(blk2)
	if err != nil {
		result.Error = err
		return result
	}

	// Transaction A: XLock on block 1
	err = txA.SetInt(blk1, 0, 1, false)
	if err != nil {
		result.Error = err
		return result
	}

	// Sleep to ensure Transaction B can lock block 2
	time.Sleep(1 * time.Second)

	// Transaction A: Attempt XLock on block 2 (this will cause deadlock)
	err = txA.SetInt(blk2, 0, 1, false)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			_ = txA.Rollback()
			result.Error = err
			result.Aborted = true
			return result
		}
		result.Error = err
		return result
	}

	err = txA.Commit()
	if err != nil {
		result.Error = err
		return result
	}
	result.Committed = true
	return result
}

// transactionDeadlockB tries to write to block 2 and then block 1
func transactionDeadlockB(fm *file.Manager, lm *log.Manager, bm *buffer.Manager, lt *concurrency.LockTable) *TransactionResult {
	result := &TransactionResult{Name: "B"}

	txB := tx.NewTransaction(fm, lm, bm, lt)
	result.TxNum = txB.TxNum()

	blk1 := file.NewBlockId("testfile", 1)
	blk2 := file.NewBlockId("testfile", 2)

	err := txB.Pin(blk1)
	if err != nil {
		result.Error = err
		return result
	}
	err = txB.Pin(blk2)
	if err != nil {
		result.Error = err
		return result
	}

	// Transaction B: XLock on block 2
	err = txB.SetInt(blk2, 0, 2, false)
	if err != nil {
		result.Error = err
		return result
	}

	// Sleep to ensure Transaction A can lock block 1
	time.Sleep(1 * time.Second)

	// Transaction B: Attempt XLock on block 1 (this will cause deadlock)
	err = txB.SetInt(blk1, 0, 2, false)
	if err != nil {
		if strings.Contains(err.Error(), "lock abort") {
			_ = txB.Rollback()
			result.Error = err
			result.Aborted = true
			return result
		}
		result.Error = err
		return result
	}

	err = txB.Commit()
	if err != nil {
		result.Error = err
		return result
	}
	result.Committed = true
	return result
}
