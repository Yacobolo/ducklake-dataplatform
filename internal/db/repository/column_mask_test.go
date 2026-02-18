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

func setupColumnMaskRepo(t *testing.T) *ColumnMaskRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewColumnMaskRepo(writeDB)
}

func TestColumnMaskRepo_CreateAndGetForTable(t *testing.T) {
	repo := setupColumnMaskRepo(t)
	ctx := context.Background()

	m, err := repo.Create(ctx, &domain.ColumnMask{
		TableID:        "t-1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
		Description:    "Mask name column",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, m.ID)
	assert.Equal(t, "t-1", m.TableID)
	assert.Equal(t, "Name", m.ColumnName)
	assert.Equal(t, "'***'", m.MaskExpression)
	assert.Equal(t, "Mask name column", m.Description)
	assert.False(t, m.CreatedAt.IsZero())

	// GetForTable.
	masks, total, err := repo.GetForTable(ctx, "t-1", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, masks, 1)
	assert.Equal(t, m.ID, masks[0].ID)
}

func TestColumnMaskRepo_Delete(t *testing.T) {
	repo := setupColumnMaskRepo(t)
	ctx := context.Background()

	m, err := repo.Create(ctx, &domain.ColumnMask{
		TableID:        "t-1",
		ColumnName:     "Email",
		MaskExpression: "'hidden@example.com'",
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, m.ID)
	require.NoError(t, err)

	masks, total, err := repo.GetForTable(ctx, "t-1", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, masks)
}

func TestColumnMaskRepo_Delete_NotFound(t *testing.T) {
	repo := setupColumnMaskRepo(t)
	ctx := context.Background()

	err := repo.Delete(ctx, "missing-mask-id")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestColumnMaskRepo_BindAndUnbind(t *testing.T) {
	repo := setupColumnMaskRepo(t)
	ctx := context.Background()

	m, err := repo.Create(ctx, &domain.ColumnMask{
		TableID:        "t-1",
		ColumnName:     "SSN",
		MaskExpression: "'XXX-XX-XXXX'",
	})
	require.NoError(t, err)

	// Bind with SeeOriginal=true.
	err = repo.Bind(ctx, &domain.ColumnMaskBinding{
		ColumnMaskID:  m.ID,
		PrincipalID:   "p-1",
		PrincipalType: "user",
		SeeOriginal:   true,
	})
	require.NoError(t, err)

	// ListBindings.
	bindings, err := repo.ListBindings(ctx, m.ID)
	require.NoError(t, err)
	assert.Len(t, bindings, 1)
	assert.Equal(t, m.ID, bindings[0].ColumnMaskID)
	assert.True(t, bindings[0].SeeOriginal)

	// Unbind.
	err = repo.Unbind(ctx, &domain.ColumnMaskBinding{
		ColumnMaskID:  m.ID,
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.NoError(t, err)

	bindings, err = repo.ListBindings(ctx, m.ID)
	require.NoError(t, err)
	assert.Empty(t, bindings)
}

func TestColumnMaskRepo_GetForTableAndPrincipal(t *testing.T) {
	repo := setupColumnMaskRepo(t)
	ctx := context.Background()

	m, err := repo.Create(ctx, &domain.ColumnMask{
		TableID:        "t-1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	require.NoError(t, err)

	// Bind with SeeOriginal=false.
	err = repo.Bind(ctx, &domain.ColumnMaskBinding{
		ColumnMaskID:  m.ID,
		PrincipalID:   "p-1",
		PrincipalType: "user",
		SeeOriginal:   false,
	})
	require.NoError(t, err)

	// GetForTableAndPrincipal.
	masks, err := repo.GetForTableAndPrincipal(ctx, "t-1", "p-1", "user")
	require.NoError(t, err)
	assert.Len(t, masks, 1)
	assert.Equal(t, "Name", masks[0].ColumnName)
	assert.Equal(t, "'***'", masks[0].MaskExpression)
	assert.False(t, masks[0].SeeOriginal)
}

func TestColumnMaskRepo_SeeOriginal(t *testing.T) {
	repo := setupColumnMaskRepo(t)
	ctx := context.Background()

	m, err := repo.Create(ctx, &domain.ColumnMask{
		TableID:        "t-1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	require.NoError(t, err)

	// Bind with SeeOriginal=true.
	err = repo.Bind(ctx, &domain.ColumnMaskBinding{
		ColumnMaskID:  m.ID,
		PrincipalID:   "p-admin",
		PrincipalType: "user",
		SeeOriginal:   true,
	})
	require.NoError(t, err)

	masks, err := repo.GetForTableAndPrincipal(ctx, "t-1", "p-admin", "user")
	require.NoError(t, err)
	require.Len(t, masks, 1)
	assert.True(t, masks[0].SeeOriginal)
}

func TestColumnMaskRepo_Pagination(t *testing.T) {
	repo := setupColumnMaskRepo(t)
	ctx := context.Background()

	for _, col := range []string{"Name", "Email", "Phone"} {
		_, err := repo.Create(ctx, &domain.ColumnMask{
			TableID:        "t-multi",
			ColumnName:     col,
			MaskExpression: "'***'",
		})
		require.NoError(t, err)
	}

	masks, total, err := repo.GetForTable(ctx, "t-multi", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)
	assert.Len(t, masks, 3)
}
