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

func setupRowFilterRepo(t *testing.T) *RowFilterRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewRowFilterRepo(writeDB)
}

func TestRowFilterRepo_CreateAndGetForTable(t *testing.T) {
	repo := setupRowFilterRepo(t)
	ctx := context.Background()

	f, err := repo.Create(ctx, &domain.RowFilter{
		TableID:     "t-1",
		FilterSQL:   `"Pclass" = 1`,
		Description: "First class only",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, f.ID)
	assert.Equal(t, "t-1", f.TableID)
	assert.Equal(t, `"Pclass" = 1`, f.FilterSQL)
	assert.Equal(t, "First class only", f.Description)
	assert.False(t, f.CreatedAt.IsZero())

	// GetForTable.
	filters, total, err := repo.GetForTable(ctx, "t-1", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, filters, 1)
	assert.Equal(t, f.ID, filters[0].ID)
}

func TestRowFilterRepo_Delete(t *testing.T) {
	repo := setupRowFilterRepo(t)
	ctx := context.Background()

	f, err := repo.Create(ctx, &domain.RowFilter{
		TableID:   "t-1",
		FilterSQL: `"Survived" = 1`,
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, f.ID)
	require.NoError(t, err)

	filters, total, err := repo.GetForTable(ctx, "t-1", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, filters)
}

func TestRowFilterRepo_BindAndUnbind(t *testing.T) {
	repo := setupRowFilterRepo(t)
	ctx := context.Background()

	f, err := repo.Create(ctx, &domain.RowFilter{
		TableID:   "t-1",
		FilterSQL: `"Pclass" = 1`,
	})
	require.NoError(t, err)

	// Bind.
	err = repo.Bind(ctx, &domain.RowFilterBinding{
		RowFilterID:   f.ID,
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.NoError(t, err)

	// ListBindings.
	bindings, err := repo.ListBindings(ctx, f.ID)
	require.NoError(t, err)
	assert.Len(t, bindings, 1)
	assert.Equal(t, f.ID, bindings[0].RowFilterID)
	assert.Equal(t, "p-1", bindings[0].PrincipalID)

	// Unbind.
	err = repo.Unbind(ctx, &domain.RowFilterBinding{
		RowFilterID:   f.ID,
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.NoError(t, err)

	bindings, err = repo.ListBindings(ctx, f.ID)
	require.NoError(t, err)
	assert.Empty(t, bindings)
}

func TestRowFilterRepo_GetForTableAndPrincipal(t *testing.T) {
	repo := setupRowFilterRepo(t)
	ctx := context.Background()

	// Create two filters on the same table.
	f1, err := repo.Create(ctx, &domain.RowFilter{
		TableID:   "t-1",
		FilterSQL: `"Pclass" = 1`,
	})
	require.NoError(t, err)

	_, err = repo.Create(ctx, &domain.RowFilter{
		TableID:   "t-1",
		FilterSQL: `"Pclass" = 2`,
	})
	require.NoError(t, err)

	// Bind only f1 to p-1.
	err = repo.Bind(ctx, &domain.RowFilterBinding{
		RowFilterID:   f1.ID,
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.NoError(t, err)

	// Should return only the bound filter.
	filters, err := repo.GetForTableAndPrincipal(ctx, "t-1", "p-1", "user")
	require.NoError(t, err)
	assert.Len(t, filters, 1)
	assert.Equal(t, `"Pclass" = 1`, filters[0].FilterSQL)
}

func TestRowFilterRepo_GetForTableAndPrincipal_Empty(t *testing.T) {
	repo := setupRowFilterRepo(t)
	ctx := context.Background()

	filters, err := repo.GetForTableAndPrincipal(ctx, "t-nonexistent", "p-1", "user")
	require.NoError(t, err)
	assert.Empty(t, filters)
}

func TestRowFilterRepo_Delete_NotFound(t *testing.T) {
	repo := setupRowFilterRepo(t)
	ctx := context.Background()

	err := repo.Delete(ctx, "nonexistent-id")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestRowFilterRepo_Unbind_NotFound(t *testing.T) {
	repo := setupRowFilterRepo(t)
	ctx := context.Background()

	err := repo.Unbind(ctx, &domain.RowFilterBinding{
		RowFilterID:   "nonexistent-filter",
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestRowFilterRepo_Pagination(t *testing.T) {
	repo := setupRowFilterRepo(t)
	ctx := context.Background()

	// Create 3 filters.
	for i := 0; i < 3; i++ {
		_, err := repo.Create(ctx, &domain.RowFilter{
			TableID:   "t-paginated",
			FilterSQL: `"col" = ` + string(rune('1'+i)),
		})
		require.NoError(t, err)
	}

	filters, total, err := repo.GetForTable(ctx, "t-paginated", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)
	assert.Len(t, filters, 3)
}
