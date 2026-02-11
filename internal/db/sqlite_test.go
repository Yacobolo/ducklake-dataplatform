package db

import (
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildDSN_Write(t *testing.T) {
	dsn := buildDSN("/tmp/test.sqlite", "write")

	assert.Contains(t, dsn, "_journal_mode=WAL")
	assert.Contains(t, dsn, "_busy_timeout=5000")
	assert.Contains(t, dsn, "_synchronous=NORMAL")
	assert.Contains(t, dsn, "_foreign_keys=on")
	assert.Contains(t, dsn, "_txlock=immediate")
	assert.True(t, strings.HasPrefix(dsn, "/tmp/test.sqlite?"))
}

func TestBuildDSN_Read(t *testing.T) {
	dsn := buildDSN("/tmp/test.sqlite", "read")

	assert.Contains(t, dsn, "_journal_mode=WAL")
	assert.Contains(t, dsn, "_busy_timeout=5000")
	assert.Contains(t, dsn, "_synchronous=NORMAL")
	assert.Contains(t, dsn, "_foreign_keys=on")
	assert.NotContains(t, dsn, "_txlock")
}

func TestOpenSQLite_InvalidMode(t *testing.T) {
	_, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"), "invalid", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid SQLite mode")
}

func TestOpenSQLite_Write(t *testing.T) {
	db, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"), "write", 0)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	// Verify WAL mode
	var journalMode string
	err = db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	require.NoError(t, err)
	assert.Equal(t, "wal", strings.ToLower(journalMode))

	// Verify busy_timeout
	var busyTimeout int
	err = db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout)
	require.NoError(t, err)
	assert.Equal(t, 5000, busyTimeout)

	// Verify pool stats
	assert.Equal(t, 1, db.Stats().MaxOpenConnections)
}

func TestOpenSQLite_Read(t *testing.T) {
	// First create the file with a write pool (sets WAL mode on file)
	path := filepath.Join(t.TempDir(), "test.db")
	wdb, err := OpenSQLite(path, "write", 0)
	require.NoError(t, err)
	wdb.Close()

	db, err := OpenSQLite(path, "read", 4)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	var journalMode string
	err = db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	require.NoError(t, err)
	assert.Equal(t, "wal", strings.ToLower(journalMode))

	assert.Equal(t, 4, db.Stats().MaxOpenConnections)
}

func TestOpenSQLite_ReadDefaultMaxOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenSQLite(path, "read", 0)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	assert.Equal(t, 4, db.Stats().MaxOpenConnections)
}

func TestOpenSQLitePair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	writeDB, readDB, err := OpenSQLitePair(path, 4)
	require.NoError(t, err)
	t.Cleanup(func() {
		writeDB.Close()
		readDB.Close()
	})

	// Write pool should have 1 connection
	assert.Equal(t, 1, writeDB.Stats().MaxOpenConnections)
	// Read pool should have 4 connections
	assert.Equal(t, 4, readDB.Stats().MaxOpenConnections)

	// Write through write pool, read through read pool
	_, err = writeDB.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
	require.NoError(t, err)

	_, err = writeDB.Exec("INSERT INTO test (val) VALUES ('hello')")
	require.NoError(t, err)

	var val string
	err = readDB.QueryRow("SELECT val FROM test WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "hello", val)
}

func TestOpenSQLitePair_ConcurrentReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	writeDB, readDB, err := OpenSQLitePair(path, 4)
	require.NoError(t, err)
	t.Cleanup(func() {
		writeDB.Close()
		readDB.Close()
	})

	// Seed some data
	_, err = writeDB.Exec("CREATE TABLE nums (n INTEGER)")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		_, err = writeDB.Exec("INSERT INTO nums (n) VALUES (?)", i)
		require.NoError(t, err)
	}

	// Launch concurrent readers — should not block each other
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var count int
			errs[idx] = readDB.QueryRow("SELECT count(*) FROM nums").Scan(&count)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		assert.NoError(t, e, "reader %d failed", i)
	}
}

func TestOpenSQLite_ForeignKeysEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenSQLite(path, "write", 0)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	var fk int
	err = db.QueryRow("PRAGMA foreign_keys").Scan(&fk)
	require.NoError(t, err)
	assert.Equal(t, 1, fk)
}

func TestOpenSQLite_InvalidPath(t *testing.T) {
	_, err := OpenSQLite("/nonexistent/dir/test.db", "write", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ping sqlite")
}

func TestOpenSQLitePair_WriteFailClosesNothing(t *testing.T) {
	// If the write pool fails to open, readDB should not be attempted
	_, _, err := OpenSQLitePair("/nonexistent/dir/test.db", 4)
	require.Error(t, err)
}

// TestOpenSQLite_BusyTimeoutPreventsErrors verifies that the busy_timeout
// setting prevents SQLITE_BUSY errors when a writer and reader access the
// database concurrently.
func TestOpenSQLite_BusyTimeoutPreventsErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	writeDB, readDB, err := OpenSQLitePair(path, 4)
	require.NoError(t, err)
	t.Cleanup(func() {
		writeDB.Close()
		readDB.Close()
	})

	_, err = writeDB.Exec("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER)")
	require.NoError(t, err)
	_, err = writeDB.Exec("INSERT INTO counter (id, n) VALUES (1, 0)")
	require.NoError(t, err)

	// Run concurrent writes and reads
	var wg sync.WaitGroup
	writeErrs := make([]error, 20)
	readErrs := make([]error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			_, writeErrs[idx] = writeDB.Exec("UPDATE counter SET n = n + 1 WHERE id = 1")
		}(i)
		go func(idx int) {
			defer wg.Done()
			var n int
			readErrs[idx] = readDB.QueryRow("SELECT n FROM counter WHERE id = 1").Scan(&n)
		}(i)
	}
	wg.Wait()

	for i, e := range writeErrs {
		assert.NoError(t, e, "writer %d failed", i)
	}
	for i, e := range readErrs {
		assert.NoError(t, e, "reader %d failed", i)
	}

	// Verify final count
	var n int
	err = readDB.QueryRow("SELECT n FROM counter WHERE id = 1").Scan(&n)
	require.NoError(t, err)
	assert.Equal(t, 20, n)
}

// verify sql.DB is interface compatible for test use
var _ interface{ Stats() sql.DBStats } = (*sql.DB)(nil)
