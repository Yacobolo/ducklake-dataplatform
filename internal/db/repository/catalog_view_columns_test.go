package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestCatalogRepo_DescribeViewColumns(t *testing.T) {
	t.Run("returns columns for a duckdb view", func(t *testing.T) {
		repo := setupCatalogRepoWithDuckDB(t)
		ctx := context.Background()

		_, err := repo.duckDB.ExecContext(ctx, `ATTACH ':memory:' AS lake`)
		require.NoError(t, err)
		_, err = repo.duckDB.ExecContext(ctx, `CREATE SCHEMA lake.analytics`)
		require.NoError(t, err)
		_, err = repo.duckDB.ExecContext(ctx, `CREATE TABLE lake.analytics.users (id INTEGER NOT NULL, nickname VARCHAR)`)
		require.NoError(t, err)
		_, err = repo.duckDB.ExecContext(ctx, `CREATE VIEW lake.analytics.user_names AS SELECT id, nickname FROM lake.analytics.users`)
		require.NoError(t, err)

		cols, err := repo.DescribeViewColumns(ctx, "analytics", "user_names")
		require.NoError(t, err)
		require.Len(t, cols, 2)
		assert.Equal(t, "id", cols[0].Name)
		assert.Equal(t, "INTEGER", cols[0].Type)
		assert.Equal(t, 0, cols[0].Position)
		assert.False(t, cols[0].Nullable)
		assert.Equal(t, "nickname", cols[1].Name)
		assert.Equal(t, "VARCHAR", cols[1].Type)
		assert.Equal(t, 1, cols[1].Position)
		assert.True(t, cols[1].Nullable)
	})

	t.Run("missing view returns not found", func(t *testing.T) {
		repo := setupCatalogRepoWithDuckDB(t)
		ctx := context.Background()

		_, err := repo.duckDB.ExecContext(ctx, `ATTACH ':memory:' AS lake`)
		require.NoError(t, err)
		_, err = repo.duckDB.ExecContext(ctx, `CREATE SCHEMA lake.analytics`)
		require.NoError(t, err)

		_, err = repo.DescribeViewColumns(ctx, "analytics", "missing_view")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}
