package notebook

import (
	"context"
	"testing"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextResolver_Resolve(t *testing.T) {
	notebooks := &testutil.MockNotebookRepo{}
	folders := &testutil.MockFolderRepo{}
	resolver := NewContextResolver(notebooks, folders)

	notebooks.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
		require.Equal(t, "nb-1", id)
		return &domain.Notebook{
			ID:        id,
			FolderID:  "folder-child",
			Name:      "Notebook",
			Owner:     "alice",
			GitPath:   strPtr("reports/notebook.yaml"),
			GitRepoID: nil,
		}, nil
	}
	folders.ListAncestorsFn = func(_ context.Context, folderID string) ([]domain.Folder, error) {
		require.Equal(t, "folder-child", folderID)
		return []domain.Folder{
			{ID: "folder-child"},
			{
				ID:                   "folder-parent",
				GitRepoID:            strPtr("repo-1"),
				GitRootPath:          strPtr("analytics/notebooks"),
				DefaultProjectID:     strPtr("project-1"),
				DefaultEnvironmentID: strPtr("env-1"),
			},
		}, nil
	}

	ctx := context.Background()
	resolved, err := resolver.Resolve(ctx, "nb-1")
	require.NoError(t, err)
	assert.Equal(t, "project-1", *resolved.EffectiveProjectID)
	assert.Equal(t, "env-1", *resolved.EffectiveEnvironmentID)
	assert.Equal(t, "repo-1", *resolved.EffectiveGitRepoID)
	assert.Equal(t, "analytics/notebooks", *resolved.EffectiveGitRootPath)
}

func TestContextResolver_ResolveNotebookOverrideWins(t *testing.T) {
	notebooks := &testutil.MockNotebookRepo{}
	folders := &testutil.MockFolderRepo{}
	resolver := NewContextResolver(notebooks, folders)

	notebooks.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
		return &domain.Notebook{
			ID:                    "nb-1",
			FolderID:              "folder-child",
			ProjectOverrideID:     strPtr("project-override"),
			EnvironmentOverrideID: strPtr("env-override"),
		}, nil
	}
	folders.ListAncestorsFn = func(_ context.Context, _ string) ([]domain.Folder, error) {
		return []domain.Folder{
			{
				ID:                   "folder-parent",
				DefaultProjectID:     strPtr("project-inherited"),
				DefaultEnvironmentID: strPtr("env-inherited"),
			},
		}, nil
	}

	resolved, err := resolver.Resolve(context.Background(), "nb-1")
	require.NoError(t, err)
	assert.Equal(t, "project-override", *resolved.EffectiveProjectID)
	assert.Equal(t, "env-override", *resolved.EffectiveEnvironmentID)
	assert.Nil(t, resolved.ProjectSourceFolderID)
	assert.Nil(t, resolved.EnvironmentSourceID)
}

func strPtr(s string) *string { return &s }
