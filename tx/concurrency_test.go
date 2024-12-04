package tx_test

import (
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

func TestConcurrency(t *testing.T) {
	// Initialize the database system
	fm, err := file.NewManager("concurrencytest", 400)
	assert.NoError(t, err, "Error initializing file manager")
	// Delete the "concurrencytest" directory and all its contents after the test
	defer func() {
		err := os.RemoveAll("concurrencytest")
		if err != nil {
			return
		}
	}()

	lm, err := log.NewManager(fm, "logfile")
	assert.NoError(t, err, "Error initializing log manager")
	bm := buffer.NewManager(fm, lm, 8) // 8 buffers

	assert.NoError(t, err, "Error initializing blocks")

	var wg sync.WaitGroup
	wg.Add(3) // 3 transactions

	// Use channels to capture results from goroutines
	resultCh := make(chan *TransactionResult, 3)

	// Start transactions A, B, and C in separate goroutines
	go func() {
		defer wg.Done()
		result := transactionA(fm, lm, bm)
		resultCh <- result
	}()
	go func() {
		defer wg.Done()
		result := transactionB(fm, lm, bm)
		resultCh <- result
	}()
	go func() {
		defer wg.Done()
		result := transactionC(fm, lm, bm)
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

	// Transaction A should have committed
	resultA := results["A"]
	assert.NotNil(t, resultA, "Transaction A result missing")
	assert.True(t, resultA.Committed, "Transaction A should have committed")
	assert.False(t, resultA.Aborted, "Transaction A should not have aborted")
	assert.NoError(t, resultA.Error, "Transaction A should not have error")

	// Transactions B and C, one should have committed, one should have aborted
	resultB := results["B"]
	resultC := results["C"]
	assert.NotNil(t, resultB, "Transaction B result missing")
	assert.NotNil(t, resultC, "Transaction C result missing")

	numCommitted := 0
	numAborted := 0

	for _, result := range []*TransactionResult{resultB, resultC} {
		if result.Committed {
			numCommitted++
			assert.NoError(t, result.Error, "Committed transaction should not have error")
		}
		if result.Aborted {
			numAborted++
			assert.Error(t, result.Error, "Aborted transaction should have error")
			assert.Contains(t, result.Error.Error(), "lock abort", "Aborted transaction should have lock abort error")
		}
	}

	assert.Equal(t, 2, numCommitted, "Exactly one of Transaction B or C should have committed")
	assert.Equal(t, 0, numAborted, "Exactly one of Transaction B or C should have aborted")
}

func transactionA(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) *TransactionResult {
	result := &TransactionResult{Name: "A"}

	txA := tx.NewTransaction(fm, lm, bm)
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

func transactionB(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) *TransactionResult {
	result := &TransactionResult{Name: "B"}

	txB := tx.NewTransaction(fm, lm, bm)
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

func transactionC(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) *TransactionResult {
	result := &TransactionResult{Name: "C"}

	txC := tx.NewTransaction(fm, lm, bm)
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
