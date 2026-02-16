package repository

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// ---------------------------------------------------------------------------
// GetTable
// ---------------------------------------------------------------------------

func TestCatalogRepo_GetTable(t *testing.T) {
	t.Run("happy path with columns", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		tableID := seedTable(t, repo.metaDB, schemaID, "users")
		seedColumn(t, repo.metaDB, tableID, "id", "INTEGER", false)
		seedColumn(t, repo.metaDB, tableID, "name", "VARCHAR", true)

		tbl, err := repo.GetTable(ctx, "public", "users")
		require.NoError(t, err)
		require.NotNil(t, tbl)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, "public", tbl.SchemaName)
		assert.Equal(t, "lake", tbl.CatalogName)
		assert.Equal(t, "MANAGED", tbl.TableType)
		assert.NotEmpty(t, tbl.TableID)
		require.Len(t, tbl.Columns, 2)
		assert.Equal(t, "id", tbl.Columns[0].Name)
		assert.Equal(t, "INTEGER", tbl.Columns[0].Type)
		assert.False(t, tbl.Columns[0].Nullable)
		assert.Equal(t, "name", tbl.Columns[1].Name)
		assert.True(t, tbl.Columns[1].Nullable)
	})

	t.Run("table not found", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()
		seedSchema(t, repo.metaDB, "public")

		_, err := repo.GetTable(ctx, "public", "nonexistent")
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})

	t.Run("schema not found", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		_, err := repo.GetTable(ctx, "no_schema", "any_table")
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})

	t.Run("soft-deleted table not found", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		_, err := repo.metaDB.Exec(
			`INSERT INTO ducklake_table (schema_id, table_name, end_snapshot) VALUES (?, ?, ?)`,
			schemaID, "old_table", 50)
		require.NoError(t, err)

		_, err = repo.GetTable(ctx, "public", "old_table")
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})

	t.Run("enriches metadata from catalog_metadata", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		seedTable(t, repo.metaDB, schemaID, "enriched_tbl")

		_, err := repo.controlDB.Exec(
			`INSERT INTO catalog_metadata (securable_type, securable_name, comment, owner)
			 VALUES ('table', 'public.enriched_tbl', 'table comment', 'bob')`)
		require.NoError(t, err)

		tbl, err := repo.GetTable(ctx, "public", "enriched_tbl")
		require.NoError(t, err)
		assert.Equal(t, "table comment", tbl.Comment)
		assert.Equal(t, "bob", tbl.Owner)
	})
}

// ---------------------------------------------------------------------------
// ListTables
// ---------------------------------------------------------------------------

func TestCatalogRepo_ListTables(t *testing.T) {
	t.Run("multiple tables in schema", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		seedTable(t, repo.metaDB, schemaID, "orders")
		seedTable(t, repo.metaDB, schemaID, "products")
		seedTable(t, repo.metaDB, schemaID, "users")
		// Soft-deleted table — should not appear.
		_, err := repo.metaDB.Exec(
			`INSERT INTO ducklake_table (schema_id, table_name, end_snapshot) VALUES (?, ?, ?)`,
			schemaID, "deleted_tbl", 99)
		require.NoError(t, err)

		tables, total, err := repo.ListTables(ctx, "public", domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, tables, 3)

		// Should be ORDER BY table_name.
		assert.Equal(t, "orders", tables[0].Name)
		assert.Equal(t, "products", tables[1].Name)
		assert.Equal(t, "users", tables[2].Name)

		for _, tbl := range tables {
			assert.Equal(t, "MANAGED", tbl.TableType)
			assert.Equal(t, "lake", tbl.CatalogName)
			assert.Equal(t, "public", tbl.SchemaName)
		}
	})

	t.Run("pagination", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "paged")
		seedTable(t, repo.metaDB, schemaID, "a")
		seedTable(t, repo.metaDB, schemaID, "b")
		seedTable(t, repo.metaDB, schemaID, "c")

		// Page 1
		tables, total, err := repo.ListTables(ctx, "paged", domain.PageRequest{MaxResults: 2})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, tables, 2)

		// Page 2
		tables, total, err = repo.ListTables(ctx, "paged", domain.PageRequest{
			MaxResults: 2,
			PageToken:  domain.EncodePageToken(2),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, tables, 1)
	})

	t.Run("nonexistent schema returns NotFoundError", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		_, _, err := repo.ListTables(ctx, "no_such", domain.PageRequest{})
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})

	t.Run("empty schema", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		seedSchema(t, repo.metaDB, "empty")

		tables, total, err := repo.ListTables(ctx, "empty", domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Empty(t, tables)
	})
}

// ---------------------------------------------------------------------------
// ListColumns
// ---------------------------------------------------------------------------

func TestCatalogRepo_ListColumns(t *testing.T) {
	t.Run("columns for a table with pagination", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		tableID := seedTable(t, repo.metaDB, schemaID, "users")
		seedColumn(t, repo.metaDB, tableID, "id", "INTEGER", false)
		seedColumn(t, repo.metaDB, tableID, "name", "VARCHAR", true)
		seedColumn(t, repo.metaDB, tableID, "email", "VARCHAR", true)

		// All columns
		cols, total, err := repo.ListColumns(ctx, "public", "users", domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, cols, 3)
		assert.Equal(t, "id", cols[0].Name)
		assert.False(t, cols[0].Nullable)
		assert.Equal(t, 0, cols[0].Position)

		// Paginated — first page
		cols, total, err = repo.ListColumns(ctx, "public", "users", domain.PageRequest{MaxResults: 2})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, cols, 2)

		// Paginated — second page
		cols, total, err = repo.ListColumns(ctx, "public", "users", domain.PageRequest{
			MaxResults: 2,
			PageToken:  domain.EncodePageToken(2),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, cols, 1)
		assert.Equal(t, "email", cols[0].Name)
	})

	t.Run("table not found", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()
		seedSchema(t, repo.metaDB, "public")

		_, _, err := repo.ListColumns(ctx, "public", "no_such_table", domain.PageRequest{})
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})

	t.Run("schema not found", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		_, _, err := repo.ListColumns(ctx, "missing", "anything", domain.PageRequest{})
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})
}

// ---------------------------------------------------------------------------
// UpdateTable
// ---------------------------------------------------------------------------

func TestCatalogRepo_UpdateTable(t *testing.T) {
	t.Run("update comment", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		seedTable(t, repo.metaDB, schemaID, "orders")

		comment := "order data"
		tbl, err := repo.UpdateTable(ctx, "public", "orders", &comment, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, "order data", tbl.Comment)
	})

	t.Run("update properties", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		seedTable(t, repo.metaDB, schemaID, "products")

		props := map[string]string{"retention": "30d"}
		tbl, err := repo.UpdateTable(ctx, "public", "products", nil, props, nil)
		require.NoError(t, err)
		assert.Equal(t, "30d", tbl.Properties["retention"])
	})

	t.Run("update owner", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		seedTable(t, repo.metaDB, schemaID, "events")

		owner := "data-team"
		tbl, err := repo.UpdateTable(ctx, "public", "events", nil, nil, &owner)
		require.NoError(t, err)
		assert.Equal(t, "data-team", tbl.Owner)
	})

	t.Run("update comment, properties, and owner together", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		seedTable(t, repo.metaDB, schemaID, "metrics")

		comment := "metrics table"
		props := map[string]string{"tier": "gold"}
		owner := "sre"
		tbl, err := repo.UpdateTable(ctx, "public", "metrics", &comment, props, &owner)
		require.NoError(t, err)
		assert.Equal(t, "metrics table", tbl.Comment)
		assert.Equal(t, "gold", tbl.Properties["tier"])
		assert.Equal(t, "sre", tbl.Owner)
	})

	t.Run("table not found", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		seedSchema(t, repo.metaDB, "public")

		comment := "nope"
		_, err := repo.UpdateTable(ctx, "public", "ghost", &comment, nil, nil)
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})
}

// ---------------------------------------------------------------------------
// UpdateColumn
// ---------------------------------------------------------------------------

func TestCatalogRepo_UpdateColumn(t *testing.T) {
	t.Run("update comment", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		tableID := seedTable(t, repo.metaDB, schemaID, "users")
		seedColumn(t, repo.metaDB, tableID, "email", "VARCHAR", true)

		comment := "user email address"
		col, err := repo.UpdateColumn(ctx, "public", "users", "email", &comment, nil)
		require.NoError(t, err)
		assert.Equal(t, "user email address", col.Comment)
		assert.Equal(t, "email", col.Name)
	})

	t.Run("update properties", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		tableID := seedTable(t, repo.metaDB, schemaID, "users")
		seedColumn(t, repo.metaDB, tableID, "ssn", "VARCHAR", true)

		props := map[string]string{"pii": "true", "mask": "hash"}
		col, err := repo.UpdateColumn(ctx, "public", "users", "ssn", nil, props)
		require.NoError(t, err)
		assert.Equal(t, "true", col.Properties["pii"])
		assert.Equal(t, "hash", col.Properties["mask"])
	})

	t.Run("column not found", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemaID := seedSchema(t, repo.metaDB, "public")
		seedTable(t, repo.metaDB, schemaID, "users")

		comment := "nope"
		_, err := repo.UpdateColumn(ctx, "public", "users", "nonexistent", &comment, nil)
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})

	t.Run("table not found", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		seedSchema(t, repo.metaDB, "public")

		comment := "nope"
		_, err := repo.UpdateColumn(ctx, "public", "ghost", "col", &comment, nil)
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})
}

// ---------------------------------------------------------------------------
// UpdateCatalog
// ---------------------------------------------------------------------------

func TestCatalogRepo_UpdateCatalog(t *testing.T) {
	t.Run("update catalog comment", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		comment := "production data lake"
		info, err := repo.UpdateCatalog(ctx, &comment)
		require.NoError(t, err)
		assert.Equal(t, "lake", info.Name)
		assert.Equal(t, "production data lake", info.Comment)
	})

	t.Run("update catalog comment to empty", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		// First set a comment.
		comment := "initial"
		_, err := repo.UpdateCatalog(ctx, &comment)
		require.NoError(t, err)

		// Update to empty string.
		empty := ""
		info, err := repo.UpdateCatalog(ctx, &empty)
		require.NoError(t, err)
		assert.Equal(t, "lake", info.Name)
		// COALESCE in upsert: empty string is still a valid (non-NULL) value.
		assert.Equal(t, "", info.Comment)
	})

	t.Run("nil comment preserves existing", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		comment := "keep me"
		_, err := repo.UpdateCatalog(ctx, &comment)
		require.NoError(t, err)

		// Pass nil — COALESCE should preserve the existing comment.
		info, err := repo.UpdateCatalog(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, "keep me", info.Comment)
	})
}
