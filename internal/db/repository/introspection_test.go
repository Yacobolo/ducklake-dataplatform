package repository

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

// introspectionIDs holds the auto-generated IDs produced by seedIntrospectionData.
type introspectionIDs struct {
	schemaPublic  int64
	schemaPrivate int64
	tableUsers    int64
	tableOrders   int64
}

// seedIntrospectionData inserts a standard set of schemas, tables, and columns
// (including soft-deleted rows) into the ducklake tables.
func seedIntrospectionData(t *testing.T, db *sql.DB) introspectionIDs {
	t.Helper()
	var ids introspectionIDs

	// Active schemas
	ids.schemaPublic = seedSchema(t, db, "public")
	ids.schemaPrivate = seedSchema(t, db, "private")

	// Soft-deleted schema (should be excluded from results)
	_, err := db.Exec(`INSERT INTO ducklake_schema (schema_name, end_snapshot) VALUES ('archived', 100)`)
	require.NoError(t, err)

	// Active tables in "public"
	ids.tableUsers = seedTable(t, db, ids.schemaPublic, "users")
	ids.tableOrders = seedTable(t, db, ids.schemaPublic, "orders")

	// Soft-deleted table
	_, err = db.Exec(`INSERT INTO ducklake_table (schema_id, table_name, end_snapshot) VALUES (?, 'deleted_table', 50)`, ids.schemaPublic)
	require.NoError(t, err)

	// Columns on "users"
	seedColumn(t, db, ids.tableUsers, "id", "INTEGER", false)
	seedColumn(t, db, ids.tableUsers, "email", "VARCHAR", true)
	seedColumn(t, db, ids.tableUsers, "name", "VARCHAR", true)

	// Soft-deleted column
	_, err = db.Exec(
		`INSERT INTO ducklake_column (table_id, column_name, column_type, end_snapshot) VALUES (?, 'old_col', 'TEXT', 99)`,
		ids.tableUsers)
	require.NoError(t, err)

	// Columns on "orders"
	seedColumn(t, db, ids.tableOrders, "order_id", "INTEGER", false)
	seedColumn(t, db, ids.tableOrders, "amount", "DOUBLE", true)

	return ids
}

func setupIntrospectionRepo(t *testing.T) (*IntrospectionRepo, *sql.DB, introspectionIDs) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	createDuckLakeTables(t, writeDB)
	ids := seedIntrospectionData(t, writeDB)
	return NewIntrospectionRepo(writeDB), writeDB, ids
}

// ---------------------------------------------------------------------------
// ListSchemas
// ---------------------------------------------------------------------------

func TestIntrospectionRepo_ListSchemas(t *testing.T) {
	tests := []struct {
		name      string
		page      domain.PageRequest
		wantCount int
		wantTotal int64
	}{
		{
			name:      "all schemas",
			page:      domain.PageRequest{},
			wantCount: 2,
			wantTotal: 2,
		},
		{
			name:      "first page size 1",
			page:      domain.PageRequest{MaxResults: 1},
			wantCount: 1,
			wantTotal: 2,
		},
		{
			name:      "second page size 1",
			page:      domain.PageRequest{MaxResults: 1, PageToken: domain.EncodePageToken(1)},
			wantCount: 1,
			wantTotal: 2,
		},
		{
			name:      "offset past end",
			page:      domain.PageRequest{MaxResults: 10, PageToken: domain.EncodePageToken(10)},
			wantCount: 0,
			wantTotal: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, _, _ := setupIntrospectionRepo(t)
			ctx := context.Background()

			schemas, total, err := repo.ListSchemas(ctx, tc.page)
			require.NoError(t, err)
			assert.Equal(t, tc.wantTotal, total)
			assert.Len(t, schemas, tc.wantCount)

			// Active schemas should never include the soft-deleted "archived"
			for _, s := range schemas {
				assert.NotEqual(t, "archived", s.Name)
				assert.NotEmpty(t, s.ID)
			}
		})
	}
}

func TestIntrospectionRepo_ListSchemas_Empty(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	createDuckLakeTables(t, writeDB)
	repo := NewIntrospectionRepo(writeDB)
	ctx := context.Background()

	schemas, total, err := repo.ListSchemas(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, schemas)
}

// ---------------------------------------------------------------------------
// ListTables
// ---------------------------------------------------------------------------

func TestIntrospectionRepo_ListTables(t *testing.T) {
	repo, _, ids := setupIntrospectionRepo(t)
	ctx := context.Background()

	schemaID := domain.DuckLakeIDToString(ids.schemaPublic)

	tests := []struct {
		name      string
		schemaID  string
		page      domain.PageRequest
		wantCount int
		wantTotal int64
	}{
		{
			name:      "all tables in public",
			schemaID:  schemaID,
			page:      domain.PageRequest{},
			wantCount: 2,
			wantTotal: 2,
		},
		{
			name:      "paginate first",
			schemaID:  schemaID,
			page:      domain.PageRequest{MaxResults: 1},
			wantCount: 1,
			wantTotal: 2,
		},
		{
			name:      "no tables in private schema",
			schemaID:  domain.DuckLakeIDToString(ids.schemaPrivate),
			page:      domain.PageRequest{},
			wantCount: 0,
			wantTotal: 0,
		},
		{
			name:      "nonexistent schema",
			schemaID:  "999",
			page:      domain.PageRequest{},
			wantCount: 0,
			wantTotal: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tables, total, err := repo.ListTables(ctx, tc.schemaID, tc.page)
			require.NoError(t, err)
			assert.Equal(t, tc.wantTotal, total)
			assert.Len(t, tables, tc.wantCount)

			for _, tbl := range tables {
				assert.NotEqual(t, "deleted_table", tbl.Name)
				assert.NotEmpty(t, tbl.ID)
				assert.NotEmpty(t, tbl.SchemaID)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetTable
// ---------------------------------------------------------------------------

func TestIntrospectionRepo_GetTable(t *testing.T) {
	repo, _, ids := setupIntrospectionRepo(t)
	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		tbl, err := repo.GetTable(ctx, domain.DuckLakeIDToString(ids.tableUsers))
		require.NoError(t, err)
		require.NotNil(t, tbl)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, domain.DuckLakeIDToString(ids.tableUsers), tbl.ID)
		assert.Equal(t, domain.DuckLakeIDToString(ids.schemaPublic), tbl.SchemaID)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := repo.GetTable(ctx, "999")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// ---------------------------------------------------------------------------
// ListColumns
// ---------------------------------------------------------------------------

func TestIntrospectionRepo_ListColumns(t *testing.T) {
	repo, _, ids := setupIntrospectionRepo(t)
	ctx := context.Background()

	tableID := domain.DuckLakeIDToString(ids.tableUsers)

	tests := []struct {
		name      string
		tableID   string
		page      domain.PageRequest
		wantCount int
		wantTotal int64
	}{
		{
			name:      "all columns on users",
			tableID:   tableID,
			page:      domain.PageRequest{},
			wantCount: 3, // id, email, name (old_col is soft-deleted)
			wantTotal: 3,
		},
		{
			name:      "paginate first column",
			tableID:   tableID,
			page:      domain.PageRequest{MaxResults: 1},
			wantCount: 1,
			wantTotal: 3,
		},
		{
			name:      "columns on orders",
			tableID:   domain.DuckLakeIDToString(ids.tableOrders),
			page:      domain.PageRequest{},
			wantCount: 2,
			wantTotal: 2,
		},
		{
			name:      "nonexistent table",
			tableID:   "999",
			page:      domain.PageRequest{},
			wantCount: 0,
			wantTotal: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cols, total, err := repo.ListColumns(ctx, tc.tableID, tc.page)
			require.NoError(t, err)
			assert.Equal(t, tc.wantTotal, total)
			assert.Len(t, cols, tc.wantCount)

			for _, c := range cols {
				assert.NotEqual(t, "old_col", c.Name)
				assert.NotEmpty(t, c.ID)
				assert.NotEmpty(t, c.TableID)
				assert.NotEmpty(t, c.Type)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetTableByName
// ---------------------------------------------------------------------------

func TestIntrospectionRepo_GetTableByName(t *testing.T) {
	repo, _, ids := setupIntrospectionRepo(t)
	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		tbl, err := repo.GetTableByName(ctx, "users")
		require.NoError(t, err)
		require.NotNil(t, tbl)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, domain.DuckLakeIDToString(ids.tableUsers), tbl.ID)
		assert.Equal(t, domain.DuckLakeIDToString(ids.schemaPublic), tbl.SchemaID)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := repo.GetTableByName(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("soft-deleted table not found", func(t *testing.T) {
		_, err := repo.GetTableByName(ctx, "deleted_table")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// ---------------------------------------------------------------------------
// GetSchemaByName
// ---------------------------------------------------------------------------

func TestIntrospectionRepo_GetSchemaByName(t *testing.T) {
	repo, _, ids := setupIntrospectionRepo(t)
	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		s, err := repo.GetSchemaByName(ctx, "public")
		require.NoError(t, err)
		require.NotNil(t, s)
		assert.Equal(t, "public", s.Name)
		assert.Equal(t, domain.DuckLakeIDToString(ids.schemaPublic), s.ID)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := repo.GetSchemaByName(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("soft-deleted schema not found", func(t *testing.T) {
		_, err := repo.GetSchemaByName(ctx, "archived")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}
