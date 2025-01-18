package server

import (
	"fmt"
	"github.com/JyotinderSingh/dropdb/buffer"
	"github.com/JyotinderSingh/dropdb/file"
	"github.com/JyotinderSingh/dropdb/log"
	"github.com/JyotinderSingh/dropdb/metadata"
	"github.com/JyotinderSingh/dropdb/tx"
	"github.com/JyotinderSingh/dropdb/tx/concurrency"
)

const (
	blockSize  = 400
	bufferSize = 8
	logFile    = "dropdb.log"
)

type DropDB struct {
	fileManager     *file.Manager
	bufferManager   *buffer.Manager
	logManager      *log.Manager
	metadataManager *metadata.Manager
	lockTable       *concurrency.LockTable
}

// NewDropDBWithOptions is a constructor that is mostly useful for debugging purposes.
func NewDropDBWithOptions(dirName string, blockSize, bufferSize int) (*DropDB, error) {
	db := &DropDB{}
	var err error

	if db.fileManager, err = file.NewManager(dirName, blockSize); err != nil {
		return nil, err
	}
	if db.logManager, err = log.NewManager(db.fileManager, logFile); err != nil {
		return nil, err
	}
	db.bufferManager = buffer.NewManager(db.fileManager, db.logManager, bufferSize)
	db.lockTable = concurrency.NewLockTable()

	return db, nil
}

// NewDropDB creates a new DropDB instance. Use this constructor for production code.
func NewDropDB(dirName string) (*DropDB, error) {
	db, err := NewDropDBWithOptions(dirName, blockSize, bufferSize)
	if err != nil {
		return nil, err
	}

	transaction := db.NewTx()
	isNew := db.fileManager.IsNew()

	if isNew {
		fmt.Printf("creating new database\n")
	} else {
		fmt.Printf("recovering existing database\n")
		if err := transaction.Recover(); err != nil {
			return nil, err
		}
	}

	if db.metadataManager, err = metadata.NewManager(isNew, transaction); err != nil {
		return nil, err
	}

	// TODO: Initialize QueryPlanner, UpdatePlanner, and Planner here.
	err = transaction.Commit()
	return db, err
}

func (db *DropDB) NewTx() *tx.Transaction {
	return tx.NewTransaction(db.fileManager, db.logManager, db.bufferManager, db.lockTable)
}

func (db *DropDB) MetadataManager() *metadata.Manager {
	return db.metadataManager
}

func (db *DropDB) FileManager() *file.Manager {
	return db.fileManager
}

func (db *DropDB) LogManager() *log.Manager {
	return db.logManager
}

func (db *DropDB) BufferManager() *buffer.Manager {
	return db.bufferManager
}
