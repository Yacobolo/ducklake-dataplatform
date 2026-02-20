package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func TestQueryJobRepo_CRUDLifecycle(t *testing.T) {
	t.Parallel()

	writeDB, _ := db.OpenTestSQLite(t)
	repo := NewQueryJobRepo(writeDB)

	created, err := repo.Create(context.Background(), &domain.QueryJob{
		PrincipalName: "alice",
		RequestID:     "req-1",
		SQLText:       "SELECT 1",
		Status:        domain.QueryJobStatusQueued,
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)

	err = repo.MarkRunning(context.Background(), created.ID)
	require.NoError(t, err)

	err = repo.MarkSucceeded(context.Background(), created.ID, []string{"id"}, [][]interface{}{{1}}, 1)
	require.NoError(t, err)

	loaded, err := repo.GetByID(context.Background(), created.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.QueryJobStatusSucceeded, loaded.Status)
	assert.Equal(t, []string{"id"}, loaded.Columns)
	assert.Equal(t, 1, loaded.RowCount)
	require.Len(t, loaded.Rows, 1)

	err = repo.Delete(context.Background(), created.ID)
	require.NoError(t, err)

	_, err = repo.GetByID(context.Background(), created.ID)
	require.Error(t, err)
}
