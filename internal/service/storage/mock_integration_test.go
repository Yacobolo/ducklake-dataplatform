//go:build integration

package storage

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/engine"
	"duck-demo/internal/testutil"
)

type mockExternalLocationRepo = testutil.MockExternalLocationRepo

// testSecretManager returns a DuckDBSecretManager wrapping the given DuckDB connection.
func testSecretManager(db *sql.DB) *engine.DuckDBSecretManager {
	return engine.NewDuckDBSecretManager(db)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// testDuckDB opens a fresh in-memory DuckDB with extensions installed.
func testDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, engine.InstallExtensions(context.Background(), db))
	return db
}
