package repository

import (
	"context"
	"testing"
	"time"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupGitRepoRepo(t *testing.T) *GitRepoRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewGitRepoRepo(writeDB)
}

func TestGitRepoRepo_CreateAndGetByID(t *testing.T) {
	repo := setupGitRepoRepo(t)
	ctx := context.Background()

	secret := "webhook-secret"
	gr := &domain.GitRepo{
		URL:           "https://github.com/test/repo.git",
		Branch:        "main",
		Path:          "/notebooks",
		AuthToken:     "ghp_token123",
		WebhookSecret: &secret,
		Owner:         "alice",
	}

	created, err := repo.Create(ctx, gr)
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.NotEmpty(t, created.ID)
	assert.Equal(t, "https://github.com/test/repo.git", created.URL)
	assert.Equal(t, "main", created.Branch)
	assert.Equal(t, "/notebooks", created.Path)
	assert.Equal(t, "alice", created.Owner)
	assert.False(t, created.CreatedAt.IsZero())

	got, err := repo.GetByID(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, created.URL, got.URL)
}

func TestGitRepoRepo_GetByID_NotFound(t *testing.T) {
	repo := setupGitRepoRepo(t)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, "nonexistent")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestGitRepoRepo_List(t *testing.T) {
	repo := setupGitRepoRepo(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := repo.Create(ctx, &domain.GitRepo{
			URL:       "https://github.com/test/repo.git",
			Branch:    "main",
			Path:      "/notebooks",
			AuthToken: "token",
			Owner:     "alice",
		})
		require.NoError(t, err)
	}

	repos, total, err := repo.List(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)
	assert.Len(t, repos, 3)
}

func TestGitRepoRepo_Delete(t *testing.T) {
	repo := setupGitRepoRepo(t)
	ctx := context.Background()

	created, err := repo.Create(ctx, &domain.GitRepo{
		URL:       "https://github.com/test/repo.git",
		Branch:    "main",
		Path:      "/notebooks",
		AuthToken: "token",
		Owner:     "alice",
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, created.ID)
	require.NoError(t, err)

	_, err = repo.GetByID(ctx, created.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestGitRepoRepo_UpdateSyncStatus(t *testing.T) {
	repo := setupGitRepoRepo(t)
	ctx := context.Background()

	created, err := repo.Create(ctx, &domain.GitRepo{
		URL:       "https://github.com/test/repo.git",
		Branch:    "main",
		Path:      "/notebooks",
		AuthToken: "token",
		Owner:     "alice",
	})
	require.NoError(t, err)

	syncTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	err = repo.UpdateSyncStatus(ctx, created.ID, "abc123def", syncTime)
	require.NoError(t, err)

	got, err := repo.GetByID(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, got.LastCommit)
	assert.Equal(t, "abc123def", *got.LastCommit)
	require.NotNil(t, got.LastSyncAt)
}
