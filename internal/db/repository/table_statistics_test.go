package repository

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupTableStatisticsRepo(t *testing.T) *TableStatisticsRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewTableStatisticsRepo(writeDB)
}

func tsPtrInt64(i int64) *int64 { return &i }

func TestTableStatisticsRepo_UpsertAndGet(t *testing.T) {
	repo := setupTableStatisticsRepo(t)
	ctx := context.Background()

	stats := &domain.TableStatistics{
		RowCount:    tsPtrInt64(100),
		SizeBytes:   tsPtrInt64(1024),
		ColumnCount: tsPtrInt64(5),
		ProfiledBy:  "admin",
	}

	err := repo.Upsert(ctx, "catalog.schema.table1", stats)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "catalog.schema.table1")
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, int64(100), *got.RowCount)
	assert.Equal(t, int64(1024), *got.SizeBytes)
	assert.Equal(t, int64(5), *got.ColumnCount)
	assert.Equal(t, "admin", got.ProfiledBy)
}

func TestTableStatisticsRepo_GetNonExistent(t *testing.T) {
	repo := setupTableStatisticsRepo(t)
	ctx := context.Background()

	got, err := repo.Get(ctx, "no.such.table")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestTableStatisticsRepo_UpsertUpdate(t *testing.T) {
	repo := setupTableStatisticsRepo(t)
	ctx := context.Background()

	// First upsert.
	err := repo.Upsert(ctx, "catalog.schema.table1", &domain.TableStatistics{
		RowCount:    tsPtrInt64(100),
		SizeBytes:   tsPtrInt64(1024),
		ColumnCount: tsPtrInt64(5),
		ProfiledBy:  "admin",
	})
	require.NoError(t, err)

	// Second upsert with updated row count.
	err = repo.Upsert(ctx, "catalog.schema.table1", &domain.TableStatistics{
		RowCount:    tsPtrInt64(200),
		SizeBytes:   tsPtrInt64(2048),
		ColumnCount: tsPtrInt64(5),
		ProfiledBy:  "admin",
	})
	require.NoError(t, err)

	got, err := repo.Get(ctx, "catalog.schema.table1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(200), *got.RowCount)
	assert.Equal(t, int64(2048), *got.SizeBytes)
}

func TestTableStatisticsRepo_Delete(t *testing.T) {
	repo := setupTableStatisticsRepo(t)
	ctx := context.Background()

	err := repo.Upsert(ctx, "catalog.schema.table1", &domain.TableStatistics{
		RowCount:   tsPtrInt64(100),
		ProfiledBy: "admin",
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, "catalog.schema.table1")
	require.NoError(t, err)

	got, err := repo.Get(ctx, "catalog.schema.table1")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestTableStatisticsRepo_NullableFields(t *testing.T) {
	repo := setupTableStatisticsRepo(t)
	ctx := context.Background()

	// Upsert with only RowCount set; SizeBytes and ColumnCount are nil.
	err := repo.Upsert(ctx, "catalog.schema.table1", &domain.TableStatistics{
		RowCount:   tsPtrInt64(42),
		ProfiledBy: "admin",
	})
	require.NoError(t, err)

	got, err := repo.Get(ctx, "catalog.schema.table1")
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, int64(42), *got.RowCount)
	assert.Nil(t, got.SizeBytes)
	assert.Nil(t, got.ColumnCount)
}
