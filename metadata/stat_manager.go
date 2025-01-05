package metadata

import (
	"github.com/JyotinderSingh/dropdb/record"
	"github.com/JyotinderSingh/dropdb/table"
	"github.com/JyotinderSingh/dropdb/tx"
	"sync"
)

type StatManager struct {
	tableManager *TableManager
	tableStats   map[string]*StatInfo
	numCalls     int
	mu           sync.Mutex
	refreshLimit int
}

// NewStatManager creates a new StatManager instance, initializing statistics by scanning the entire database.
func NewStatManager(tableManager *TableManager, transaction *tx.Transaction, refreshLimit int) (*StatManager, error) {
	statMgr := &StatManager{
		tableManager: tableManager,
		tableStats:   make(map[string]*StatInfo),
		refreshLimit: refreshLimit,
	}
	if err := statMgr.RefreshStatistics(transaction); err != nil {
		return nil, err
	}
	return statMgr, nil
}

// GetStatInfo returns statistical information about the specified table.
// It refreshes statistics periodically based on the refreshLimit.
func (sm *StatManager) GetStatInfo(tableName string, layout *record.Layout, transaction *tx.Transaction) (*StatInfo, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numCalls++
	if sm.numCalls > sm.refreshLimit {
		// Call the internal refresh that expects the lock to already be held
		if err := sm._refreshStatistics(transaction); err != nil {
			return nil, err
		}
	}

	if statInfo, exists := sm.tableStats[tableName]; exists {
		return statInfo, nil
	}

	// Calculate statistics if not already available
	statInfo, err := sm.calcTableStats(tableName, layout, transaction)
	if err != nil {
		return nil, err
	}
	sm.tableStats[tableName] = statInfo
	return statInfo, nil
}

// RefreshStatistics publicly forces a refresh of all table statistics.
// This is useful if something external triggers a refresh.
func (sm *StatManager) RefreshStatistics(transaction *tx.Transaction) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm._refreshStatistics(transaction)
}

// _refreshStatistics recalculates statistics for all tables in the database.
// It assumes the caller already holds sm.mu.
func (sm *StatManager) _refreshStatistics(transaction *tx.Transaction) error {
	// Since the caller already holds the lock, do NOT lock here.

	sm.tableStats = make(map[string]*StatInfo)
	sm.numCalls = 0

	tableCatalogLayout, err := sm.tableManager.GetLayout(tableCatalogTable, transaction)
	if err != nil {
		return err
	}
	tableCatalogTableScan, err := table.NewTableScan(transaction, tableCatalogTable, tableCatalogLayout)
	if err != nil {
		return err
	}
	defer tableCatalogTableScan.Close()

	for {
		hasNext, err := tableCatalogTableScan.Next()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		tblName, err := tableCatalogTableScan.GetString(tableNameField)
		if err != nil {
			return err
		}

		layout, err := sm.tableManager.GetLayout(tblName, transaction)
		if err != nil {
			return err
		}

		statInfo, err := sm.calcTableStats(tblName, layout, transaction)
		if err != nil {
			return err
		}
		sm.tableStats[tblName] = statInfo
	}

	return nil
}

// calcTableStats calculates the number of records, blocks, and distinct values for a specific table.
func (sm *StatManager) calcTableStats(tableName string, layout *record.Layout, transaction *tx.Transaction) (*StatInfo, error) {
	numRecords := 0
	numBlocks := 0
	distinctValues := make(map[string]map[any]interface{}) // field name -> distinct values

	for _, field := range layout.Schema().Fields() {
		distinctValues[field] = make(map[any]interface{})
	}

	ts, err := table.NewTableScan(transaction, tableName, layout)
	if err != nil {
		return nil, err
	}
	defer ts.Close()

	for {
		hasNext, err := ts.Next()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		numRecords++
		rid := ts.GetRecordID()
		if rid.BlockNumber() >= numBlocks {
			numBlocks = rid.BlockNumber() + 1
		}

		// Track distinct values for each field
		for _, field := range layout.Schema().Fields() {
			val, err := ts.GetVal(field)
			if err != nil {
				return nil, err
			}
			distinctValues[field][val] = struct{}{}
		}
	}

	distinctCounts := make(map[string]int)
	for field, values := range distinctValues {
		distinctCounts[field] = len(values)
	}

	return NewStatInfo(numBlocks, numRecords, distinctCounts), nil
}
