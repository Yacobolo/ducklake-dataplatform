// Package db provides database connectivity helpers and migration support.
package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"
)

// SQLite DSN parameters for production hardening.
const (
	defaultBusyTimeout = "5000" // 5 seconds
	defaultSynchronous = "NORMAL"
	defaultJournalMode = "WAL"
)

// OpenSQLite opens a *sql.DB pool for the given SQLite file path.
//
// mode controls write-safety and pool sizing:
//   - "write": MaxOpenConns=1, MaxIdleConns=1, includes _txlock=immediate
//   - "read":  MaxOpenConns=maxOpen (use 0 for default of 4), no _txlock
//
// Both modes set WAL journal, busy_timeout=5000ms, synchronous=NORMAL,
// and foreign_keys=on.
func OpenSQLite(path string, mode string, maxOpen int) (*sql.DB, error) {
	if mode != "read" && mode != "write" {
		return nil, fmt.Errorf("invalid SQLite mode %q: must be \"read\" or \"write\"", mode)
	}

	dsn := buildDSN(path, mode)

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite (%s): %w", mode, err)
	}

	switch mode {
	case "write":
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	case "read":
		if maxOpen <= 0 {
			maxOpen = 4
		}
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxOpen)
	}
	db.SetConnMaxLifetime(time.Hour)

	// Verify the connection is usable.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite (%s): %w", mode, err)
	}

	return db, nil
}

// OpenSQLitePair opens both a write pool (MaxOpenConns=1) and a read pool
// for the same SQLite file. This is the recommended way to configure SQLite
// for concurrent access from a Go HTTP server.
//
// readMaxOpen controls the read pool size (0 defaults to 4).
func OpenSQLitePair(path string, readMaxOpen int) (writeDB, readDB *sql.DB, err error) {
	writeDB, err = OpenSQLite(path, "write", 0)
	if err != nil {
		return nil, nil, err
	}

	readDB, err = OpenSQLite(path, "read", readMaxOpen)
	if err != nil {
		_ = writeDB.Close()
		return nil, nil, err
	}

	return writeDB, readDB, nil
}

// buildDSN constructs a SQLite DSN with hardened parameters.
func buildDSN(path string, mode string) string {
	params := url.Values{}
	params.Set("_journal_mode", defaultJournalMode)
	params.Set("_busy_timeout", defaultBusyTimeout)
	params.Set("_synchronous", defaultSynchronous)
	params.Set("_foreign_keys", "on")

	if mode == "write" {
		params.Set("_txlock", "immediate")
	}

	return path + "?" + params.Encode()
}
