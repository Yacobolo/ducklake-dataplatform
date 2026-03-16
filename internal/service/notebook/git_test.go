package notebook

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

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
		gitRepoRepo := &testutil.MockGitRepoRepo{}
		notebookRepo := newMemoryNotebookRepo()
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

		createdRepo := &domain.GitRepo{
			ID:     "repo-1",
			URL:    repoDir,
			Branch: "master",
			Owner:  "alice",
		}
		gitRepoRepo.GetByIDFn = func(_ context.Context, id string) (*domain.GitRepo, error) {
			require.Equal(t, "repo-1", id)
			return createdRepo, nil
		}
		gitRepoRepo.UpdateSyncStatusFn = func(_ context.Context, id string, commitSHA string, syncedAt time.Time) error {
			require.Equal(t, "repo-1", id)
			require.NotEmpty(t, commitSHA)
			createdRepo.LastCommit = &commitSHA
			createdRepo.LastSyncAt = &syncedAt
			return nil
		}

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

type memoryNotebookRepo struct {
	notebooks map[string]*domain.Notebook
	cells     map[string][]*domain.Cell
}

func newMemoryNotebookRepo() *memoryNotebookRepo {
	return &memoryNotebookRepo{
		notebooks: make(map[string]*domain.Notebook),
		cells:     make(map[string][]*domain.Cell),
	}
}

func (r *memoryNotebookRepo) CreateNotebook(_ context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
	created := *nb
	if created.ID == "" {
		created.ID = domain.NewID()
	}
	created.CreatedAt = time.Now().UTC()
	created.UpdatedAt = created.CreatedAt
	r.notebooks[created.ID] = cloneNotebook(&created)
	return cloneNotebook(&created), nil
}

func (r *memoryNotebookRepo) GetNotebook(_ context.Context, id string) (*domain.Notebook, error) {
	nb, ok := r.notebooks[id]
	if !ok {
		return nil, domain.ErrNotFound("notebook not found")
	}
	return cloneNotebook(nb), nil
}

func (r *memoryNotebookRepo) ListNotebooks(_ context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error) {
	items := make([]domain.Notebook, 0, len(r.notebooks))
	for _, nb := range r.notebooks {
		if owner != nil && nb.Owner != *owner {
			continue
		}
		items = append(items, *cloneNotebook(nb))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	total := int64(len(items))
	start := page.Offset()
	if start > len(items) {
		start = len(items)
	}
	end := start + page.Limit()
	if end > len(items) {
		end = len(items)
	}
	return items[start:end], total, nil
}

func (r *memoryNotebookRepo) UpdateNotebook(_ context.Context, _ string, _ domain.UpdateNotebookRequest) (*domain.Notebook, error) {
	panic("unexpected call")
}

func (r *memoryNotebookRepo) UpdateNotebookSync(_ context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
	existing, ok := r.notebooks[nb.ID]
	if !ok {
		return nil, domain.ErrNotFound("notebook not found")
	}
	existing.Name = nb.Name
	existing.Description = cloneStringPtr(nb.Description)
	existing.Owner = nb.Owner
	existing.GitRepoID = cloneStringPtr(nb.GitRepoID)
	existing.GitPath = cloneStringPtr(nb.GitPath)
	existing.UpdatedAt = time.Now().UTC()
	return cloneNotebook(existing), nil
}

func (r *memoryNotebookRepo) DeleteNotebook(_ context.Context, id string) error {
	delete(r.notebooks, id)
	delete(r.cells, id)
	return nil
}

func (r *memoryNotebookRepo) CreateCell(_ context.Context, cell *domain.Cell) (*domain.Cell, error) {
	created := *cell
	if created.ID == "" {
		created.ID = domain.NewID()
	}
	created.CreatedAt = time.Now().UTC()
	created.UpdatedAt = created.CreatedAt
	r.cells[created.NotebookID] = append(r.cells[created.NotebookID], cloneCell(&created))
	return cloneCell(&created), nil
}

func (r *memoryNotebookRepo) GetCell(_ context.Context, id string) (*domain.Cell, error) {
	for _, cells := range r.cells {
		for _, cell := range cells {
			if cell.ID == id {
				return cloneCell(cell), nil
			}
		}
	}
	return nil, domain.ErrNotFound("cell not found")
}

func (r *memoryNotebookRepo) ListCells(_ context.Context, notebookID string) ([]domain.Cell, error) {
	cells := r.cells[notebookID]
	items := make([]domain.Cell, 0, len(cells))
	for _, cell := range cells {
		items = append(items, *cloneCell(cell))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Position < items[j].Position })
	return items, nil
}

func (r *memoryNotebookRepo) UpdateCell(_ context.Context, _ string, _ domain.UpdateCellRequest) (*domain.Cell, error) {
	panic("unexpected call")
}

func (r *memoryNotebookRepo) UpdateCellSync(_ context.Context, cell *domain.Cell) (*domain.Cell, error) {
	cells := r.cells[cell.NotebookID]
	for i, existing := range cells {
		if existing.ID != cell.ID {
			continue
		}
		updated := *cell
		updated.CreatedAt = existing.CreatedAt
		updated.UpdatedAt = time.Now().UTC()
		r.cells[cell.NotebookID][i] = cloneCell(&updated)
		return cloneCell(&updated), nil
	}
	return nil, domain.ErrNotFound("cell not found")
}

func (r *memoryNotebookRepo) DeleteCell(_ context.Context, id string) error {
	for notebookID, cells := range r.cells {
		for i, cell := range cells {
			if cell.ID != id {
				continue
			}
			r.cells[notebookID] = append(cells[:i], cells[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *memoryNotebookRepo) UpdateCellResult(_ context.Context, cellID string, result *string) error {
	for _, cells := range r.cells {
		for _, cell := range cells {
			if cell.ID == cellID {
				cell.LastResult = cloneStringPtr(result)
				cell.UpdatedAt = time.Now().UTC()
				return nil
			}
		}
	}
	return domain.ErrNotFound("cell not found")
}

func (r *memoryNotebookRepo) ReorderCells(_ context.Context, notebookID string, cellIDs []string) error {
	current := r.cells[notebookID]
	byID := make(map[string]*domain.Cell, len(current))
	for _, cell := range current {
		byID[cell.ID] = cell
	}
	reordered := make([]*domain.Cell, 0, len(cellIDs))
	for position, id := range cellIDs {
		cell, ok := byID[id]
		if !ok {
			return domain.ErrNotFound("cell not found")
		}
		cell.Position = position
		reordered = append(reordered, cell)
	}
	r.cells[notebookID] = reordered
	return nil
}

func (r *memoryNotebookRepo) GetMaxPosition(_ context.Context, notebookID string) (int, error) {
	maxPosition := -1
	for _, cell := range r.cells[notebookID] {
		if cell.Position > maxPosition {
			maxPosition = cell.Position
		}
	}
	return maxPosition, nil
}

func cloneNotebook(nb *domain.Notebook) *domain.Notebook {
	if nb == nil {
		return nil
	}
	cloned := *nb
	cloned.Description = cloneStringPtr(nb.Description)
	cloned.GitRepoID = cloneStringPtr(nb.GitRepoID)
	cloned.GitPath = cloneStringPtr(nb.GitPath)
	return &cloned
}

func cloneCell(cell *domain.Cell) *domain.Cell {
	if cell == nil {
		return nil
	}
	cloned := *cell
	cloned.Name = cloneStringPtr(cell.Name)
	cloned.Test = cloneNotebookCellTest(cell.Test)
	cloned.VisualSpec = cloneVisualSpec(cell.VisualSpec)
	cloned.LastResult = cloneStringPtr(cell.LastResult)
	return &cloned
}

func cloneNotebookCellTest(test *domain.NotebookCellTestConfig) *domain.NotebookCellTestConfig {
	if test == nil {
		return nil
	}
	cloned := *test
	return &cloned
}

func cloneVisualSpec(spec *domain.VisualSpec) *domain.VisualSpec {
	if spec == nil {
		return nil
	}
	cloned := *spec
	if spec.ChartType != nil {
		chartType := *spec.ChartType
		cloned.ChartType = &chartType
	}
	cloned.Encodings = spec.Encodings
	if spec.Encodings.X != nil {
		value := *spec.Encodings.X
		cloned.Encodings.X = &value
	}
	if spec.Encodings.Y != nil {
		value := *spec.Encodings.Y
		cloned.Encodings.Y = &value
	}
	if spec.Encodings.Series != nil {
		value := *spec.Encodings.Series
		cloned.Encodings.Series = &value
	}
	if spec.Encodings.Label != nil {
		value := *spec.Encodings.Label
		cloned.Encodings.Label = &value
	}
	if spec.Encodings.Value != nil {
		value := *spec.Encodings.Value
		cloned.Encodings.Value = &value
	}
	if spec.Encodings.Secondary != nil {
		value := *spec.Encodings.Secondary
		cloned.Encodings.Secondary = &value
	}
	if spec.Legend != nil {
		value := *spec.Legend
		cloned.Legend = &value
	}
	if spec.Stacked != nil {
		value := *spec.Stacked
		cloned.Stacked = &value
	}
	return &cloned
}

func cloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
