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

func setupViewRepo(t *testing.T) *ViewRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewViewRepo(writeDB)
}

func viewPtrStr(s string) *string { return &s }

func TestViewRepo_CreateAndGetByName(t *testing.T) {
	repo := setupViewRepo(t)
	ctx := context.Background()

	comment := viewPtrStr("test")
	created, err := repo.Create(ctx, &domain.ViewDetail{
		SchemaID:       "schema-001",
		Name:           "my_view",
		ViewDefinition: "SELECT 1",
		Comment:        comment,
		Properties:     map[string]string{"key": "val"},
		Owner:          "admin",
		SourceTables:   []string{"t1"},
	})
	require.NoError(t, err)
	require.NotNil(t, created)

	assert.NotEmpty(t, created.ID)
	assert.Equal(t, "schema-001", created.SchemaID)
	assert.Equal(t, "my_view", created.Name)
	assert.Equal(t, "SELECT 1", created.ViewDefinition)
	require.NotNil(t, created.Comment)
	assert.Equal(t, "test", *created.Comment)
	assert.Equal(t, map[string]string{"key": "val"}, created.Properties)
	assert.Equal(t, "admin", created.Owner)
	assert.Equal(t, []string{"t1"}, created.SourceTables)
	assert.False(t, created.CreatedAt.IsZero())
	assert.False(t, created.UpdatedAt.IsZero())
	assert.Nil(t, created.DeletedAt)

	// GetByName should return the same view.
	got, err := repo.GetByName(ctx, "schema-001", "my_view")
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, "schema-001", got.SchemaID)
	assert.Equal(t, "my_view", got.Name)
	assert.Equal(t, "SELECT 1", got.ViewDefinition)
	require.NotNil(t, got.Comment)
	assert.Equal(t, "test", *got.Comment)
	assert.Equal(t, map[string]string{"key": "val"}, got.Properties)
	assert.Equal(t, "admin", got.Owner)
	assert.Equal(t, []string{"t1"}, got.SourceTables)
}

func TestViewRepo_List(t *testing.T) {
	repo := setupViewRepo(t)
	ctx := context.Background()

	for _, name := range []string{"view_alpha", "view_beta"} {
		_, err := repo.Create(ctx, &domain.ViewDetail{
			SchemaID:       "schema-001",
			Name:           name,
			ViewDefinition: "SELECT 1",
			Owner:          "admin",
		})
		require.NoError(t, err)
	}

	views, total, err := repo.List(ctx, "schema-001", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, views, 2)
	// ListViews orders by name.
	assert.Equal(t, "view_alpha", views[0].Name)
	assert.Equal(t, "view_beta", views[1].Name)

	// A different schema should return zero results.
	views2, total2, err := repo.List(ctx, "schema-other", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total2)
	assert.Empty(t, views2)
}

func TestViewRepo_Delete(t *testing.T) {
	repo := setupViewRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.ViewDetail{
		SchemaID:       "schema-001",
		Name:           "doomed_view",
		ViewDefinition: "SELECT 1",
		Owner:          "admin",
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, "schema-001", "doomed_view")
	require.NoError(t, err)

	// GetByName should return NotFoundError after soft delete.
	_, err = repo.GetByName(ctx, "schema-001", "doomed_view")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestViewRepo_Update(t *testing.T) {
	repo := setupViewRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.ViewDetail{
		SchemaID:       "schema-001",
		Name:           "updatable_view",
		ViewDefinition: "SELECT 1",
		Comment:        viewPtrStr("original"),
		Owner:          "admin",
	})
	require.NoError(t, err)

	newComment := "updated comment"
	newDef := "SELECT 2"
	updated, err := repo.Update(ctx, "schema-001", "updatable_view", &newComment, nil, &newDef)
	require.NoError(t, err)
	require.NotNil(t, updated)

	require.NotNil(t, updated.Comment)
	assert.Equal(t, "updated comment", *updated.Comment)
	assert.Equal(t, "SELECT 2", updated.ViewDefinition)
	assert.Equal(t, "admin", updated.Owner)

	// Verify via GetByName.
	got, err := repo.GetByName(ctx, "schema-001", "updatable_view")
	require.NoError(t, err)
	require.NotNil(t, got.Comment)
	assert.Equal(t, "updated comment", *got.Comment)
	assert.Equal(t, "SELECT 2", got.ViewDefinition)
}

func TestViewRepo_UpdateProperties(t *testing.T) {
	repo := setupViewRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.ViewDetail{
		SchemaID:       "schema-001",
		Name:           "props_view",
		ViewDefinition: "SELECT 1",
		Properties:     map[string]string{"key1": "val1"},
		Owner:          "admin",
	})
	require.NoError(t, err)

	newProps := map[string]string{"key2": "val2", "key3": "val3"}
	updated, err := repo.Update(ctx, "schema-001", "props_view", nil, newProps, nil)
	require.NoError(t, err)
	require.NotNil(t, updated)

	// Properties should be fully replaced.
	assert.Equal(t, map[string]string{"key2": "val2", "key3": "val3"}, updated.Properties)
	_, hasOld := updated.Properties["key1"]
	assert.False(t, hasOld, "old property key1 should not be present after replacement")

	// Verify via GetByName.
	got, err := repo.GetByName(ctx, "schema-001", "props_view")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"key2": "val2", "key3": "val3"}, got.Properties)
}
