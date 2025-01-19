package driver

import (
	"database/sql"
	"database/sql/driver"
	"github.com/JyotinderSingh/dropdb/server"
)

const dbName = "dropdb"

// Register the driver when this package is imported.
func init() {
	sql.Register(dbName, &DropDBDriver{})
}

// DropDBDriver implements database/sql/driver.Driver.
var _ driver.Driver = (*DropDBDriver)(nil)

type DropDBDriver struct{}

// Open is the entry point. The directory is the path to the DB directory.
func (d *DropDBDriver) Open(directory string) (driver.Conn, error) {
	db, err := server.NewDropDB(directory)
	if err != nil {
		return nil, err
	}
	return &DropDBConn{
		db: db,
		// We do not open a transaction here. We'll open a new one for each statement (auto-commit).
	}, nil
}
