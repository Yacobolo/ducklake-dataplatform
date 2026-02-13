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

func setupExternalTableRepo(t *testing.T) *ExternalTableRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewExternalTableRepo(writeDB)
}

func TestExternalTable_CreateAndGet(t *testing.T) {
	repo := setupExternalTableRepo(t)
	ctx := context.Background()

	et, err := repo.Create(ctx, &domain.ExternalTableRecord{
		SchemaName:   "analytics",
		TableName:    "events",
		FileFormat:   "parquet",
		SourcePath:   "s3://bucket/data/events.parquet",
		LocationName: "my_location",
		Comment:      "test table",
		Owner:        "admin",
		Columns: []domain.ExternalTableColumn{
			{ColumnName: "id", ColumnType: "INTEGER", Position: 0},
			{ColumnName: "name", ColumnType: "VARCHAR", Position: 1},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, et)
	assert.NotEmpty(t, et.ID)
	assert.Equal(t, "analytics", et.SchemaName)
	assert.Equal(t, "events", et.TableName)
	assert.Equal(t, "parquet", et.FileFormat)
	assert.Equal(t, "s3://bucket/data/events.parquet", et.SourcePath)
	assert.Equal(t, "my_location", et.LocationName)
	assert.Equal(t, "test table", et.Comment)
	assert.Equal(t, "admin", et.Owner)
	assert.Len(t, et.Columns, 2)
	assert.Equal(t, "id", et.Columns[0].ColumnName)
	assert.Equal(t, "INTEGER", et.Columns[0].ColumnType)
	assert.Equal(t, 0, et.Columns[0].Position)
	assert.Equal(t, "name", et.Columns[1].ColumnName)

	// GetByName
	found, err := repo.GetByName(ctx, "analytics", "events")
	require.NoError(t, err)
	assert.Equal(t, et.ID, found.ID)
	assert.Len(t, found.Columns, 2)

	// GetByID
	byID, err := repo.GetByID(ctx, et.ID)
	require.NoError(t, err)
	assert.Equal(t, et.TableName, byID.TableName)

	// GetByTableName
	byTableName, err := repo.GetByTableName(ctx, "events")
	require.NoError(t, err)
	assert.Equal(t, et.ID, byTableName.ID)
}

func TestExternalTable_List(t *testing.T) {
	repo := setupExternalTableRepo(t)
	ctx := context.Background()

	// Create two tables in "analytics" and one in "other"
	for _, name := range []string{"alpha", "beta"} {
		_, err := repo.Create(ctx, &domain.ExternalTableRecord{
			SchemaName:   "analytics",
			TableName:    name,
			FileFormat:   "parquet",
			SourcePath:   "s3://bucket/" + name + ".parquet",
			LocationName: "loc",
		})
		require.NoError(t, err)
	}
	_, err := repo.Create(ctx, &domain.ExternalTableRecord{
		SchemaName:   "other",
		TableName:    "gamma",
		FileFormat:   "csv",
		SourcePath:   "s3://bucket/gamma.csv",
		LocationName: "loc",
	})
	require.NoError(t, err)

	// List analytics
	tables, total, err := repo.List(ctx, "analytics", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, tables, 2)
	assert.Equal(t, "alpha", tables[0].TableName) // sorted

	// ListAll
	all, err := repo.ListAll(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

func TestExternalTable_Delete(t *testing.T) {
	repo := setupExternalTableRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.ExternalTableRecord{
		SchemaName:   "analytics",
		TableName:    "events",
		FileFormat:   "parquet",
		SourcePath:   "s3://bucket/events.parquet",
		LocationName: "loc",
	})
	require.NoError(t, err)

	// Soft delete
	err = repo.Delete(ctx, "analytics", "events")
	require.NoError(t, err)

	// Should not be found anymore
	_, err = repo.GetByName(ctx, "analytics", "events")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestExternalTable_DeleteBySchema(t *testing.T) {
	repo := setupExternalTableRepo(t)
	ctx := context.Background()

	for _, name := range []string{"alpha", "beta"} {
		_, err := repo.Create(ctx, &domain.ExternalTableRecord{
			SchemaName:   "analytics",
			TableName:    name,
			FileFormat:   "parquet",
			SourcePath:   "s3://bucket/" + name + ".parquet",
			LocationName: "loc",
		})
		require.NoError(t, err)
	}

	err := repo.DeleteBySchema(ctx, "analytics")
	require.NoError(t, err)

	tables, total, err := repo.List(ctx, "analytics", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, tables)
}

func TestExternalTable_UniqueConstraint(t *testing.T) {
	repo := setupExternalTableRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.ExternalTableRecord{
		SchemaName:   "analytics",
		TableName:    "events",
		FileFormat:   "parquet",
		SourcePath:   "s3://bucket/events.parquet",
		LocationName: "loc",
	})
	require.NoError(t, err)

	// Duplicate should fail
	_, err = repo.Create(ctx, &domain.ExternalTableRecord{
		SchemaName:   "analytics",
		TableName:    "events",
		FileFormat:   "csv",
		SourcePath:   "s3://bucket/events.csv",
		LocationName: "loc",
	})
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}

func TestExternalTable_IDIsString(t *testing.T) {
	et := &domain.ExternalTableRecord{ID: "42"}
	assert.Equal(t, "42", et.ID)
}
