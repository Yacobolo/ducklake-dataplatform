package notebook

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupGitService(t *testing.T) (*GitService, *testutil.MockGitRepoRepo, *testutil.MockNotebookRepo, *testutil.MockAuditRepo) {
	t.Helper()
	repo := &testutil.MockGitRepoRepo{}
	notebooks := &testutil.MockNotebookRepo{}
	audit := &testutil.MockAuditRepo{}
	svc := NewGitService(repo, notebooks, audit)
	return svc, repo, notebooks, audit
}

// === CreateGitRepo ===

func TestGitService_CreateGitRepo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
			svc, repo, _, audit := setupGitService(t)
		ctx := context.Background()

		repo.CreateFn = func(_ context.Context, r *domain.GitRepo) (*domain.GitRepo, error) {
			assert.NotEmpty(t, r.ID, "ID should be generated")
			assert.Equal(t, "https://github.com/org/repo.git", r.URL)
			assert.Equal(t, "main", r.Branch)
			assert.Equal(t, "/notebooks", r.Path)
			assert.Equal(t, "tok-123", r.AuthToken)
			assert.Equal(t, "alice", r.Owner)
			return r, nil
		}

		result, err := svc.CreateGitRepo(ctx, "alice", domain.CreateGitRepoRequest{
			URL:       "https://github.com/org/repo.git",
			Branch:    "main",
			Path:      "/notebooks",
			AuthToken: "tok-123",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, result.ID)
		assert.Equal(t, "alice", result.Owner)
		assert.Equal(t, "https://github.com/org/repo.git", result.URL)
		assert.True(t, audit.HasAction("CREATE_GIT_REPO"))
	})

	t.Run("validation_error_empty_url", func(t *testing.T) {
			svc, _, _, _ := setupGitService(t)
		ctx := context.Background()

		_, err := svc.CreateGitRepo(ctx, "alice", domain.CreateGitRepoRequest{
			URL:    "",
			Branch: "main",
		})
		require.Error(t, err)
		var validationErr *domain.ValidationError
		require.ErrorAs(t, err, &validationErr)
		assert.Contains(t, validationErr.Message, "url is required")
	})

	t.Run("validation_error_empty_branch", func(t *testing.T) {
			svc, _, _, _ := setupGitService(t)
		ctx := context.Background()

		_, err := svc.CreateGitRepo(ctx, "alice", domain.CreateGitRepoRequest{
			URL:    "https://github.com/org/repo.git",
			Branch: "",
		})
		require.Error(t, err)
		var validationErr *domain.ValidationError
		require.ErrorAs(t, err, &validationErr)
		assert.Contains(t, validationErr.Message, "branch is required")
	})
}

// === GetGitRepo ===

func TestGitService_GetGitRepo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
			svc, repo, _, _ := setupGitService(t)
		ctx := context.Background()

		repo.GetByIDFn = func(_ context.Context, id string) (*domain.GitRepo, error) {
			return &domain.GitRepo{
				ID:     id,
				URL:    "https://github.com/org/repo.git",
				Branch: "main",
				Owner:  "alice",
			}, nil
		}

		result, err := svc.GetGitRepo(ctx, "repo-1")
		require.NoError(t, err)
		assert.Equal(t, "repo-1", result.ID)
		assert.Equal(t, "https://github.com/org/repo.git", result.URL)
	})

	t.Run("not_found", func(t *testing.T) {
			svc, repo, _, _ := setupGitService(t)
		ctx := context.Background()

		repo.GetByIDFn = func(_ context.Context, _ string) (*domain.GitRepo, error) {
			return nil, domain.ErrNotFound("git repo not found")
		}

		_, err := svc.GetGitRepo(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		require.ErrorAs(t, err, &notFound)
	})

	t.Run("secure read denies non-owner", func(t *testing.T) {
			svc, repo, _, _ := setupGitService(t)
		ctx := context.Background()

		repo.GetByIDFn = func(_ context.Context, id string) (*domain.GitRepo, error) {
			return &domain.GitRepo{ID: id, Owner: "alice"}, nil
		}

		_, err := svc.GetGitRepoForPrincipal(ctx, "bob", false, "repo-1")
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		require.ErrorAs(t, err, &accessDenied)
	})
}

// === ListGitRepos ===

func TestGitService_ListGitRepos(t *testing.T) {
	t.Run("success", func(t *testing.T) {
			svc, repo, _, _ := setupGitService(t)
		ctx := context.Background()

		want := []domain.GitRepo{
			{ID: "repo-1", URL: "https://github.com/org/a.git", Branch: "main", Owner: "alice"},
			{ID: "repo-2", URL: "https://github.com/org/b.git", Branch: "dev", Owner: "bob"},
		}
		var wantTotal int64 = 2

		repo.ListFn = func(_ context.Context, page domain.PageRequest) ([]domain.GitRepo, int64, error) {
			assert.Equal(t, 10, page.MaxResults)
			return want, wantTotal, nil
		}

		repos, total, err := svc.ListGitRepos(ctx, domain.PageRequest{MaxResults: 10})
		require.NoError(t, err)
		assert.Equal(t, wantTotal, total)
		assert.Equal(t, want, repos)
	})
}

// === DeleteGitRepo ===

func TestGitService_DeleteGitRepo(t *testing.T) {
	t.Run("owner_can_delete", func(t *testing.T) {
			svc, repo, _, audit := setupGitService(t)
		ctx := context.Background()

		repo.GetByIDFn = func(_ context.Context, id string) (*domain.GitRepo, error) {
			return &domain.GitRepo{ID: id, Owner: "alice"}, nil
		}
		repo.DeleteFn = func(_ context.Context, id string) error {
			assert.Equal(t, "repo-1", id)
			return nil
		}

		err := svc.DeleteGitRepo(ctx, "alice", false, "repo-1")
		require.NoError(t, err)
		assert.True(t, audit.HasAction("DELETE_GIT_REPO"))
	})

	t.Run("admin_can_delete_others", func(t *testing.T) {
			svc, repo, _, audit := setupGitService(t)
		ctx := context.Background()

		repo.GetByIDFn = func(_ context.Context, id string) (*domain.GitRepo, error) {
			return &domain.GitRepo{ID: id, Owner: "alice"}, nil
		}
		repo.DeleteFn = func(_ context.Context, id string) error {
			assert.Equal(t, "repo-1", id)
			return nil
		}

		err := svc.DeleteGitRepo(ctx, "admin-user", true, "repo-1")
		require.NoError(t, err)
		assert.True(t, audit.HasAction("DELETE_GIT_REPO"))
	})

	t.Run("non_owner_non_admin_denied", func(t *testing.T) {
			svc, repo, _, _ := setupGitService(t)
		ctx := context.Background()

		repo.GetByIDFn = func(_ context.Context, id string) (*domain.GitRepo, error) {
			return &domain.GitRepo{ID: id, Owner: "alice"}, nil
		}

		err := svc.DeleteGitRepo(ctx, "bob", false, "repo-1")
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		require.ErrorAs(t, err, &accessDenied)
	})

	t.Run("not_found", func(t *testing.T) {
			svc, repo, _, _ := setupGitService(t)
		ctx := context.Background()

		repo.GetByIDFn = func(_ context.Context, _ string) (*domain.GitRepo, error) {
			return nil, domain.ErrNotFound("git repo not found")
		}

		err := svc.DeleteGitRepo(ctx, "alice", false, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		require.ErrorAs(t, err, &notFound)
	})
}

// === SyncGitRepo ===

func TestGitService_SyncGitRepo(t *testing.T) {
	t.Run("returns_not_implemented_error", func(t *testing.T) {
		svc, repo, _, _ := setupGitService(t)
		ctx := context.Background()

		repo.GetByIDFn = func(_ context.Context, id string) (*domain.GitRepo, error) {
			return &domain.GitRepo{ID: id, Owner: "alice"}, nil
		}

		result, err := svc.SyncGitRepo(ctx, "bob", false, "repo-1")
		assert.Nil(t, result)
		require.Error(t, err)
		var deniedErr *domain.AccessDeniedError
		require.ErrorAs(t, err, &deniedErr)
	})

	t.Run("not_found_repo", func(t *testing.T) {
		svc, repo, _, _ := setupGitService(t)
		ctx := context.Background()

		repo.GetByIDFn = func(_ context.Context, _ string) (*domain.GitRepo, error) {
			return nil, domain.ErrNotFound("git repo not found")
		}

		result, err := svc.SyncGitRepo(ctx, "alice", false, "nonexistent")
		assert.Nil(t, result)
		require.Error(t, err)
		var notFound *domain.NotFoundError
		require.ErrorAs(t, err, &notFound)
	})

	t.Run("syncs declarative notebook from local repo", func(t *testing.T) {
		ctx := context.Background()
		writeDB, _ := internaldb.OpenTestSQLite(t)
		gitRepoRepo := repository.NewGitRepoRepo(writeDB)
		notebookRepo := repository.NewNotebookRepo(writeDB)
		auditRepo := &testutil.MockAuditRepo{}
		svc := NewGitService(gitRepoRepo, notebookRepo, auditRepo)

		repoDir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(repoDir, "notebooks"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(repoDir, "notebooks", "sales.yaml"), []byte(`
apiVersion: duck/v1
kind: Notebook
metadata:
  name: sales
spec:
  description: Sales notebook
  cells:
    - type: sql
      name: output
      role: output
      content: SELECT 1 AS value
      visual_spec:
        kind: metric
        encodings:
          value:
            field: value
`), 0o644))

		repo, err := git.PlainInit(repoDir, false)
		require.NoError(t, err)
		worktree, err := repo.Worktree()
		require.NoError(t, err)
		_, err = worktree.Add("notebooks/sales.yaml")
		require.NoError(t, err)
		_, err = worktree.Commit("add notebook", &git.CommitOptions{
			Author: &object.Signature{Name: "codex", Email: "codex@example.com"},
		})
		require.NoError(t, err)

		createdRepo, err := gitRepoRepo.Create(ctx, &domain.GitRepo{
			URL:    repoDir,
			Branch: "master",
			Owner:  "alice",
		})
		require.NoError(t, err)

		result, err := svc.SyncGitRepo(ctx, "alice", false, createdRepo.ID)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 1, result.NotebooksCreated)
		assert.NotEmpty(t, result.CommitSHA)
		assert.True(t, auditRepo.HasAction("SYNC_GIT_REPO"))

		items, total, err := notebookRepo.ListNotebooks(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
		require.NoError(t, err)
		require.Equal(t, int64(1), total)
		require.Len(t, items, 1)
		assert.Equal(t, "sales", items[0].Name)
		require.NotNil(t, items[0].GitRepoID)
		require.NotNil(t, items[0].GitPath)
		assert.Equal(t, createdRepo.ID, *items[0].GitRepoID)
		assert.Equal(t, "notebooks/sales.yaml", *items[0].GitPath)

		cells, err := notebookRepo.ListCells(ctx, items[0].ID)
		require.NoError(t, err)
		require.Len(t, cells, 1)
		assert.Equal(t, domain.CellRoleOutput, cells[0].Role)
		require.NotNil(t, cells[0].VisualSpec)
		assert.Equal(t, domain.VisualOutputMetric, cells[0].VisualSpec.Kind)
	})
}
