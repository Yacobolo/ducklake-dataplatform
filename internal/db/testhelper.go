package db

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// OpenTestSQLite opens a hardened SQLite write/read pool pair in t.TempDir(),
// runs all pending migrations on the write pool, and registers cleanup.
//
// Tests that don't need the read/write split can use writeDB for everything.
func OpenTestSQLite(t *testing.T) (writeDB, readDB *sql.DB) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "test.sqlite")

	writeDB, readDB, err := OpenSQLitePair(path, 4)
	if err != nil {
		t.Fatalf("open test sqlite: %v", err)
	}
	t.Cleanup(func() {
		_ = readDB.Close()
		_ = writeDB.Close()
	})

	if err := RunMigrations(writeDB); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	return writeDB, readDB
}
