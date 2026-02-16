package repository

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// ---------------------------------------------------------------------------
// Shared test helpers for CatalogRepo tests
// ---------------------------------------------------------------------------

// setupCatalogRepo creates a CatalogRepo backed by a real SQLite DB.
// It uses the same DB for both metaDB (ducklake_* tables) and controlDB
// (catalog_metadata, column_metadata — created by migrations).
// duckDB is nil because we only test read/update operations.
func setupCatalogRepo(t *testing.T) *CatalogRepo {
	t.Helper()

	writeDB, _ := internaldb.OpenTestSQLite(t)

	// Create ducklake_* tables with ALL columns the CatalogRepo queries use.
	// The existing createDuckLakeTables helper (introspection_test.go) lacks
	// path/path_is_relative/nulls_allowed/ducklake_metadata, so we use our own.
	createCatalogDuckLakeTables(t, writeDB)

	q := dbstore.New(writeDB)
	return NewCatalogRepo(writeDB, writeDB, q, nil, "lake", nil, slog.Default())
}

// createCatalogDuckLakeTables creates the full ducklake_* tables that
// the CatalogRepo queries expect, including path and metadata columns.
func createCatalogDuckLakeTables(t *testing.T, db *sql.DB) {
	t.Helper()

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ducklake_metadata (
			key   TEXT PRIMARY KEY,
			value TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS ducklake_schema (
			schema_id        INTEGER PRIMARY KEY AUTOINCREMENT,
			schema_name      TEXT NOT NULL,
			path             TEXT,
			path_is_relative INTEGER DEFAULT 0,
			end_snapshot     INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS ducklake_table (
			table_id         INTEGER PRIMARY KEY AUTOINCREMENT,
			schema_id        INTEGER NOT NULL,
			table_name       TEXT NOT NULL,
			path             TEXT,
			path_is_relative INTEGER DEFAULT 0,
			end_snapshot     INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS ducklake_column (
			column_id     INTEGER PRIMARY KEY AUTOINCREMENT,
			table_id      INTEGER NOT NULL,
			column_name   TEXT NOT NULL,
			column_type   TEXT NOT NULL,
			nulls_allowed INTEGER DEFAULT 1,
			end_snapshot  INTEGER
		)`,
	}
	for _, stmt := range stmts {
		_, err := db.Exec(stmt)
		require.NoError(t, err, "create ducklake table: %s", stmt)
	}
}

// seedSchema inserts a row into ducklake_schema and returns its ID.
func seedSchema(t *testing.T, db *sql.DB, name string) int64 {
	t.Helper()
	res, err := db.Exec(
		`INSERT INTO ducklake_schema (schema_name) VALUES (?)`, name)
	require.NoError(t, err)
	id, err := res.LastInsertId()
	require.NoError(t, err)
	return id
}

// seedTable inserts a row into ducklake_table and returns its ID.
func seedTable(t *testing.T, db *sql.DB, schemaID int64, name string) int64 {
	t.Helper()
	res, err := db.Exec(
		`INSERT INTO ducklake_table (schema_id, table_name) VALUES (?, ?)`, schemaID, name)
	require.NoError(t, err)
	id, err := res.LastInsertId()
	require.NoError(t, err)
	return id
}

// seedColumn inserts a row into ducklake_column.
func seedColumn(t *testing.T, db *sql.DB, tableID int64, name, colType string, nullable bool) {
	t.Helper()
	n := 1
	if !nullable {
		n = 0
	}
	_, err := db.Exec(
		`INSERT INTO ducklake_column (table_id, column_name, column_type, nulls_allowed) VALUES (?, ?, ?, ?)`,
		tableID, name, colType, n)
	require.NoError(t, err)
}

// createDuckLakeTables is a package-wide alias used by introspection and search
// tests. It delegates to createCatalogDuckLakeTables which includes the full
// column set (path, path_is_relative, nulls_allowed, ducklake_metadata).
func createDuckLakeTables(t *testing.T, db *sql.DB) {
	t.Helper()
	createCatalogDuckLakeTables(t, db)
}

// ptrStr returns a pointer to s. Utility for test call-sites.
func ptrStr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// Tests: ptrToStr (unexported package-level function)
// ---------------------------------------------------------------------------

func TestCatalogHelpers_PtrToStr(t *testing.T) {
	tests := []struct {
		name string
		in   *string
		want string
	}{
		{name: "nil returns empty", in: nil, want: ""},
		{name: "non-nil returns value", in: ptrStr("hello"), want: "hello"},
		{name: "empty string pointer", in: ptrStr(""), want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ptrToStr(tc.in))
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: resolveSchemaID
// ---------------------------------------------------------------------------

func TestCatalogHelpers_ResolveSchemaID(t *testing.T) {
	repo := setupCatalogRepo(t)
	ctx := context.Background()

	t.Run("existing schema returns ID", func(t *testing.T) {
		id := seedSchema(t, repo.metaDB, "sales")

		got, err := repo.resolveSchemaID(ctx, "sales")
		require.NoError(t, err)
		assert.Equal(t, id, got)
	})

	t.Run("non-existent schema returns NotFoundError", func(t *testing.T) {
		_, err := repo.resolveSchemaID(ctx, "no_such_schema")
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})

	t.Run("deleted schema (end_snapshot set) returns NotFoundError", func(t *testing.T) {
		_, err := repo.metaDB.Exec(
			`INSERT INTO ducklake_schema (schema_name, end_snapshot) VALUES (?, ?)`,
			"deleted_schema", 42)
		require.NoError(t, err)

		_, err = repo.resolveSchemaID(ctx, "deleted_schema")
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})
}

// ---------------------------------------------------------------------------
// Tests: loadColumns
// ---------------------------------------------------------------------------

func TestCatalogHelpers_LoadColumns(t *testing.T) {
	repo := setupCatalogRepo(t)
	ctx := context.Background()

	schemaID := seedSchema(t, repo.metaDB, "default")

	t.Run("returns columns ordered by column_id", func(t *testing.T) {
		tableID := seedTable(t, repo.metaDB, schemaID, "users")
		seedColumn(t, repo.metaDB, tableID, "id", "INTEGER", false)
		seedColumn(t, repo.metaDB, tableID, "name", "VARCHAR", true)
		seedColumn(t, repo.metaDB, tableID, "email", "VARCHAR", true)

		cols, err := repo.loadColumns(ctx, domain.DuckLakeIDToString(tableID))
		require.NoError(t, err)
		require.Len(t, cols, 3)

		assert.Equal(t, "id", cols[0].Name)
		assert.Equal(t, "INTEGER", cols[0].Type)
		assert.Equal(t, 0, cols[0].Position)
		assert.False(t, cols[0].Nullable)

		assert.Equal(t, "name", cols[1].Name)
		assert.Equal(t, "VARCHAR", cols[1].Type)
		assert.Equal(t, 1, cols[1].Position)
		assert.True(t, cols[1].Nullable)

		assert.Equal(t, "email", cols[2].Name)
		assert.Equal(t, 2, cols[2].Position)
	})

	t.Run("empty table returns nil slice", func(t *testing.T) {
		tableID := seedTable(t, repo.metaDB, schemaID, "empty_tbl")

		cols, err := repo.loadColumns(ctx, domain.DuckLakeIDToString(tableID))
		require.NoError(t, err)
		assert.Nil(t, cols)
	})

	t.Run("excludes soft-deleted columns", func(t *testing.T) {
		tableID := seedTable(t, repo.metaDB, schemaID, "with_deleted_col")
		seedColumn(t, repo.metaDB, tableID, "active_col", "TEXT", true)
		_, err := repo.metaDB.Exec(
			`INSERT INTO ducklake_column (table_id, column_name, column_type, end_snapshot) VALUES (?, ?, ?, ?)`,
			tableID, "old_col", "TEXT", 99)
		require.NoError(t, err)

		cols, err := repo.loadColumns(ctx, domain.DuckLakeIDToString(tableID))
		require.NoError(t, err)
		require.Len(t, cols, 1)
		assert.Equal(t, "active_col", cols[0].Name)
	})
}

// ---------------------------------------------------------------------------
// Tests: resolveStoragePath
// ---------------------------------------------------------------------------

func TestCatalogHelpers_ResolveStoragePath(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		dataPath       string
		schemaPath     sql.NullString
		tablePath      sql.NullString
		tablePathIsRel sql.NullInt64
		want           string
	}{
		{
			name:      "table absolute path takes precedence",
			dataPath:  "s3://bucket/data/",
			tablePath: sql.NullString{String: "s3://other/tbl/", Valid: true},
			want:      "s3://other/tbl/",
		},
		{
			name:           "table relative path prepends data_path",
			dataPath:       "s3://bucket/data/",
			tablePath:      sql.NullString{String: "tables/orders/", Valid: true},
			tablePathIsRel: sql.NullInt64{Int64: 1, Valid: true},
			want:           "s3://bucket/data/tables/orders/",
		},
		{
			name:       "schema path used when no table path",
			dataPath:   "s3://bucket/data/",
			schemaPath: sql.NullString{String: "s3://schema-bucket/", Valid: true},
			want:       "s3://schema-bucket/",
		},
		{
			name:     "falls back to data_path",
			dataPath: "s3://bucket/data/",
			want:     "s3://bucket/data/",
		},
		{
			name: "all empty returns empty",
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a fresh repo for each test case to set data_path independently.
			writeDB, _ := internaldb.OpenTestSQLite(t)
			createCatalogDuckLakeTables(t, writeDB)
			q := dbstore.New(writeDB)
			repo := NewCatalogRepo(writeDB, writeDB, q, nil, "lake", nil, slog.Default())

			if tc.dataPath != "" {
				_, err := writeDB.Exec(
					`INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', ?)`, tc.dataPath)
				require.NoError(t, err)
			}

			got := repo.resolveStoragePath(ctx, tc.schemaPath, tc.tablePath, tc.tablePathIsRel)
			assert.Equal(t, tc.want, got)
		})
	}
}
