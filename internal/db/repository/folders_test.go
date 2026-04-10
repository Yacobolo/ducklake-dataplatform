package repository

import (
	"context"
	"testing"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupFolderRepo(t *testing.T) *FolderRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewFolderRepo(writeDB)
}

func TestFolderRepo_EnsurePersonalWorkspaceRoot(t *testing.T) {
	repo := setupFolderRepo(t)
	ctx := context.Background()

	root, err := repo.EnsurePersonalWorkspaceRoot(ctx, "alice")
	require.NoError(t, err)
	require.NotNil(t, root)
	assert.Equal(t, "alice", root.Owner)
	assert.Equal(t, "Home", root.Name)
	assert.Equal(t, domain.FolderSystemRoleWorkspaceRoot, *root.SystemRole)
	assert.NotEmpty(t, root.WorkspaceID)

	same, err := repo.EnsurePersonalWorkspaceRoot(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, root.ID, same.ID)
}

func TestFolderRepo_ListAncestors(t *testing.T) {
	repo := setupFolderRepo(t)
	ctx := context.Background()

	root, err := repo.EnsurePersonalWorkspaceRoot(ctx, "alice")
	require.NoError(t, err)

	parentID := root.ID
	child, err := repo.Create(ctx, &domain.Folder{
		WorkspaceID:    root.WorkspaceID,
		Name:           "Finance",
		Owner:          "alice",
		ParentFolderID: &parentID,
	})
	require.NoError(t, err)

	childID := child.ID
	grandchild, err := repo.Create(ctx, &domain.Folder{
		WorkspaceID:    root.WorkspaceID,
		Name:           "Q1",
		Owner:          "alice",
		ParentFolderID: &childID,
	})
	require.NoError(t, err)

	ancestors, err := repo.ListAncestors(ctx, grandchild.ID)
	require.NoError(t, err)
	require.Len(t, ancestors, 3)
	assert.Equal(t, grandchild.ID, ancestors[0].ID)
	assert.Equal(t, child.ID, ancestors[1].ID)
	assert.Equal(t, root.ID, ancestors[2].ID)
}

func TestFolderRepo_ListByOwnerAndUpdate(t *testing.T) {
	repo := setupFolderRepo(t)
	ctx := context.Background()

	root, err := repo.EnsurePersonalWorkspaceRoot(ctx, "alice")
	require.NoError(t, err)

	parentID := root.ID
	created, err := repo.Create(ctx, &domain.Folder{
		WorkspaceID:    root.WorkspaceID,
		Name:           "Finance",
		Owner:          "alice",
		ParentFolderID: &parentID,
	})
	require.NoError(t, err)

	items, err := repo.ListByOwner(ctx, "alice")
	require.NoError(t, err)
	require.Len(t, items, 2)

	updated, err := repo.Update(ctx, created.ID, domain.UpdateFolderRequest{
		Name: ptrString("Finance Core"),
	})
	require.NoError(t, err)
	assert.Equal(t, "Finance Core", updated.Name)
}

func TestFolderRepo_Delete(t *testing.T) {
	repo := setupFolderRepo(t)
	ctx := context.Background()

	root, err := repo.EnsurePersonalWorkspaceRoot(ctx, "alice")
	require.NoError(t, err)

	parentID := root.ID
	created, err := repo.Create(ctx, &domain.Folder{
		WorkspaceID:    root.WorkspaceID,
		Name:           "To Delete",
		Owner:          "alice",
		ParentFolderID: &parentID,
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, created.ID)
	require.NoError(t, err)

	_, err = repo.GetByID(ctx, created.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func ptrString(value string) *string {
	return &value
}
