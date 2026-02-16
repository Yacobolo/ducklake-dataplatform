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
// GetSchema
// ---------------------------------------------------------------------------

func TestCatalogRepo_GetSchema(t *testing.T) {
	repo := setupCatalogRepo(t)
	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		seedSchema(t, repo.metaDB, "analytics")

		s, err := repo.GetSchema(ctx, "analytics")
		require.NoError(t, err)
		require.NotNil(t, s)
		assert.Equal(t, "analytics", s.Name)
		assert.Equal(t, "lake", s.CatalogName)
		assert.NotEmpty(t, s.SchemaID)
	})

	t.Run("not found returns NotFoundError", func(t *testing.T) {
		_, err := repo.GetSchema(ctx, "nonexistent")
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})

	t.Run("soft-deleted schema not found", func(t *testing.T) {
		_, err := repo.metaDB.ExecContext(ctx,
			`INSERT INTO ducklake_schema (schema_name, end_snapshot) VALUES (?, ?)`,
			"old_schema", 10)
		require.NoError(t, err)

		_, err = repo.GetSchema(ctx, "old_schema")
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})

	t.Run("enriches metadata from catalog_metadata", func(t *testing.T) {
		seedSchema(t, repo.metaDB, "enriched")

		// Insert catalog_metadata for the schema.
		_, err := repo.controlDB.ExecContext(ctx,
			`INSERT INTO catalog_metadata (securable_type, securable_name, comment, owner)
			 VALUES ('schema', 'enriched', 'my comment', 'alice')`)
		require.NoError(t, err)

		s, err := repo.GetSchema(ctx, "enriched")
		require.NoError(t, err)
		assert.Equal(t, "my comment", s.Comment)
		assert.Equal(t, "alice", s.Owner)
	})
}

// ---------------------------------------------------------------------------
// ListSchemas
// ---------------------------------------------------------------------------

func TestCatalogRepo_ListSchemas(t *testing.T) {
	t.Run("multiple schemas", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		seedSchema(t, repo.metaDB, "alpha")
		seedSchema(t, repo.metaDB, "beta")
		seedSchema(t, repo.metaDB, "gamma")
		// Soft-deleted — should not appear.
		_, err := repo.metaDB.ExecContext(ctx,
			`INSERT INTO ducklake_schema (schema_name, end_snapshot) VALUES ('deleted', 5)`)
		require.NoError(t, err)

		schemas, total, err := repo.ListSchemas(ctx, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, schemas, 3)

		// Results are ORDER BY schema_name.
		assert.Equal(t, "alpha", schemas[0].Name)
		assert.Equal(t, "beta", schemas[1].Name)
		assert.Equal(t, "gamma", schemas[2].Name)

		for _, s := range schemas {
			assert.Equal(t, "lake", s.CatalogName)
			assert.NotEmpty(t, s.SchemaID)
		}
	})

	t.Run("pagination", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		seedSchema(t, repo.metaDB, "a")
		seedSchema(t, repo.metaDB, "b")
		seedSchema(t, repo.metaDB, "c")

		// Page 1
		schemas, total, err := repo.ListSchemas(ctx, domain.PageRequest{MaxResults: 2})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, schemas, 2)
		assert.Equal(t, "a", schemas[0].Name)
		assert.Equal(t, "b", schemas[1].Name)

		// Page 2
		schemas, total, err = repo.ListSchemas(ctx, domain.PageRequest{
			MaxResults: 2,
			PageToken:  domain.EncodePageToken(2),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		require.Len(t, schemas, 1)
		assert.Equal(t, "c", schemas[0].Name)
	})

	t.Run("empty database", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		schemas, total, err := repo.ListSchemas(ctx, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Nil(t, schemas)
	})
}

// ---------------------------------------------------------------------------
// UpdateSchema
// ---------------------------------------------------------------------------

func TestCatalogRepo_UpdateSchema(t *testing.T) {
	t.Run("update comment", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()
		seedSchema(t, repo.metaDB, "sales")

		comment := "sales data"
		updated, err := repo.UpdateSchema(ctx, "sales", &comment, nil)
		require.NoError(t, err)
		assert.Equal(t, "sales data", updated.Comment)
	})

	t.Run("update properties", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()
		seedSchema(t, repo.metaDB, "warehouse")

		props := map[string]string{"env": "prod", "team": "platform"}
		updated, err := repo.UpdateSchema(ctx, "warehouse", nil, props)
		require.NoError(t, err)
		assert.Equal(t, "prod", updated.Properties["env"])
		assert.Equal(t, "platform", updated.Properties["team"])
	})

	t.Run("update comment and properties together", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()
		seedSchema(t, repo.metaDB, "mixed")

		comment := "updated"
		props := map[string]string{"key": "val"}
		updated, err := repo.UpdateSchema(ctx, "mixed", &comment, props)
		require.NoError(t, err)
		assert.Equal(t, "updated", updated.Comment)
		assert.Equal(t, "val", updated.Properties["key"])
	})

	t.Run("nonexistent schema returns NotFoundError", func(t *testing.T) {
		repo := setupCatalogRepo(t)
		ctx := context.Background()

		comment := "nope"
		_, err := repo.UpdateSchema(ctx, "ghost", &comment, nil)
		require.Error(t, err)
		var nf *domain.NotFoundError
		assert.ErrorAs(t, err, &nf)
	})
}

// ---------------------------------------------------------------------------
// SetSchemaStoragePath
// ---------------------------------------------------------------------------

func TestCatalogRepo_SetSchemaStoragePath(t *testing.T) {
	repo := setupCatalogRepo(t)
	ctx := context.Background()

	t.Run("set path and verify read-back", func(t *testing.T) {
		schemaID := seedSchema(t, repo.metaDB, "custom_path")
		idStr := domain.DuckLakeIDToString(schemaID)

		err := repo.SetSchemaStoragePath(ctx, idStr, "s3://my-bucket/schemas/custom/")
		require.NoError(t, err)

		// Verify the path was set correctly by reading from ducklake_schema directly.
		var path string
		var pathIsRelative int64
		err = repo.metaDB.QueryRowContext(ctx,
			`SELECT path, path_is_relative FROM ducklake_schema WHERE schema_id = ?`, schemaID).
			Scan(&path, &pathIsRelative)
		require.NoError(t, err)
		assert.Equal(t, "s3://my-bucket/schemas/custom/", path)
		assert.Equal(t, int64(0), pathIsRelative, "path_is_relative should be 0 (absolute)")
	})

	t.Run("overwrite existing path", func(t *testing.T) {
		schemaID := seedSchema(t, repo.metaDB, "overwrite_path")
		idStr := domain.DuckLakeIDToString(schemaID)

		err := repo.SetSchemaStoragePath(ctx, idStr, "s3://first/")
		require.NoError(t, err)

		err = repo.SetSchemaStoragePath(ctx, idStr, "s3://second/")
		require.NoError(t, err)

		var path string
		err = repo.metaDB.QueryRowContext(ctx,
			`SELECT path FROM ducklake_schema WHERE schema_id = ?`, schemaID).
			Scan(&path)
		require.NoError(t, err)
		assert.Equal(t, "s3://second/", path)
	})
}
