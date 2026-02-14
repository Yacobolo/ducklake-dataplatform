package notebook

import (
	"context"
	"testing"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptrStr(s string) *string { return &s }

func ptrInt(i int) *int { return &i }

func setupNotebookService(t *testing.T) (*Service, *testutil.MockNotebookRepo, *testutil.MockAuditRepo) {
	t.Helper()
	repo := &testutil.MockNotebookRepo{}
	audit := &testutil.MockAuditRepo{}
	svc := New(repo, audit)
	return svc, repo, audit
}

// === CreateNotebook ===

func TestNotebookService_CreateNotebook(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		svc, repo, audit := setupNotebookService(t)
		ctx := context.Background()

		repo.CreateNotebookFn = func(_ context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
			assert.Equal(t, "Test NB", nb.Name)
			assert.Equal(t, "alice", nb.Owner)
			return &domain.Notebook{ID: "nb-1", Name: nb.Name, Owner: nb.Owner}, nil
		}

		result, err := svc.CreateNotebook(ctx, "alice", domain.CreateNotebookRequest{Name: "Test NB"})
		require.NoError(t, err)
		assert.Equal(t, "nb-1", result.ID)
		assert.True(t, audit.HasAction("CREATE_NOTEBOOK"))
	})

	t.Run("validation error empty name", func(t *testing.T) {
		svc, _, _ := setupNotebookService(t)
		ctx := context.Background()

		_, err := svc.CreateNotebook(ctx, "alice", domain.CreateNotebookRequest{Name: ""})
		require.Error(t, err)
		var validationErr *domain.ValidationError
		assert.ErrorAs(t, err, &validationErr)
	})
}

// === GetNotebook ===

func TestNotebookService_GetNotebook(t *testing.T) {
	t.Run("success with cells", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}
		repo.ListCellsFn = func(_ context.Context, notebookID string) ([]domain.Cell, error) {
			return []domain.Cell{
				{ID: "cell-1", NotebookID: notebookID, CellType: domain.CellTypeSQL, Content: "SELECT 1"},
			}, nil
		}

		nb, cells, err := svc.GetNotebook(ctx, "nb-1")
		require.NoError(t, err)
		assert.Equal(t, "nb-1", nb.ID)
		assert.Len(t, cells, 1)
	})

	t.Run("not found", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return nil, domain.ErrNotFound("not found")
		}

		_, _, err := svc.GetNotebook(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// === UpdateNotebook ===

func TestNotebookService_UpdateNotebook(t *testing.T) {
	t.Run("owner can update", func(t *testing.T) {
		svc, repo, audit := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Name: "Old", Owner: "alice"}, nil
		}
		repo.UpdateNotebookFn = func(_ context.Context, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: *req.Name, Owner: "alice"}, nil
		}

		result, err := svc.UpdateNotebook(ctx, "alice", false, "nb-1", domain.UpdateNotebookRequest{Name: ptrStr("New")})
		require.NoError(t, err)
		assert.Equal(t, "New", result.Name)
		assert.True(t, audit.HasAction("UPDATE_NOTEBOOK"))
	})

	t.Run("admin can update others notebook", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Name: "Old", Owner: "alice"}, nil
		}
		repo.UpdateNotebookFn = func(_ context.Context, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: *req.Name, Owner: "alice"}, nil
		}

		_, err := svc.UpdateNotebook(ctx, "bob-admin", true, "nb-1", domain.UpdateNotebookRequest{Name: ptrStr("AdminUpdate")})
		require.NoError(t, err)
	})

	t.Run("non-owner non-admin denied", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Name: "Old", Owner: "alice"}, nil
		}

		_, err := svc.UpdateNotebook(ctx, "bob", false, "nb-1", domain.UpdateNotebookRequest{Name: ptrStr("Denied")})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === DeleteNotebook ===

func TestNotebookService_DeleteNotebook(t *testing.T) {
	t.Run("owner can delete", func(t *testing.T) {
		svc, repo, audit := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}
		repo.DeleteNotebookFn = func(_ context.Context, _ string) error { return nil }

		err := svc.DeleteNotebook(ctx, "alice", false, "nb-1")
		require.NoError(t, err)
		assert.True(t, audit.HasAction("DELETE_NOTEBOOK"))
	})

	t.Run("non-owner non-admin denied", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}

		err := svc.DeleteNotebook(ctx, "bob", false, "nb-1")
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === CreateCell ===

func TestNotebookService_CreateCell(t *testing.T) {
	t.Run("success with auto position", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}
		repo.GetMaxPositionFn = func(_ context.Context, _ string) (int, error) {
			return 2, nil
		}
		repo.CreateCellFn = func(_ context.Context, cell *domain.Cell) (*domain.Cell, error) {
			assert.Equal(t, 3, cell.Position) // maxPos + 1
			return &domain.Cell{ID: "cell-1", NotebookID: "nb-1", Position: cell.Position}, nil
		}

		result, err := svc.CreateCell(ctx, "alice", false, "nb-1", domain.CreateCellRequest{
			CellType: domain.CellTypeSQL,
			Content:  "SELECT 1",
		})
		require.NoError(t, err)
		assert.Equal(t, 3, result.Position)
	})

	t.Run("explicit position", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}
		repo.CreateCellFn = func(_ context.Context, cell *domain.Cell) (*domain.Cell, error) {
			assert.Equal(t, 5, cell.Position)
			return &domain.Cell{ID: "cell-1", Position: 5}, nil
		}

		_, err := svc.CreateCell(ctx, "alice", false, "nb-1", domain.CreateCellRequest{
			CellType: domain.CellTypeSQL,
			Content:  "SELECT 1",
			Position: ptrInt(5),
		})
		require.NoError(t, err)
	})

	t.Run("invalid cell type", func(t *testing.T) {
		svc, _, _ := setupNotebookService(t)
		ctx := context.Background()

		_, err := svc.CreateCell(ctx, "alice", false, "nb-1", domain.CreateCellRequest{
			CellType: "invalid",
			Content:  "SELECT 1",
		})
		require.Error(t, err)
		var validationErr *domain.ValidationError
		assert.ErrorAs(t, err, &validationErr)
	})

	t.Run("non-owner non-admin denied", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}

		_, err := svc.CreateCell(ctx, "bob", false, "nb-1", domain.CreateCellRequest{
			CellType: domain.CellTypeSQL,
			Content:  "SELECT 1",
		})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === UpdateCell ===

func TestNotebookService_UpdateCell(t *testing.T) {
	t.Run("owner can update", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetCellFn = func(_ context.Context, _ string) (*domain.Cell, error) {
			return &domain.Cell{ID: "cell-1", NotebookID: "nb-1"}, nil
		}
		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}
		repo.UpdateCellFn = func(_ context.Context, id string, req domain.UpdateCellRequest) (*domain.Cell, error) {
			return &domain.Cell{ID: id, Content: *req.Content}, nil
		}

		result, err := svc.UpdateCell(ctx, "alice", false, "cell-1", domain.UpdateCellRequest{Content: ptrStr("SELECT 2")})
		require.NoError(t, err)
		assert.Equal(t, "SELECT 2", result.Content)
	})

	t.Run("non-owner non-admin denied", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetCellFn = func(_ context.Context, _ string) (*domain.Cell, error) {
			return &domain.Cell{ID: "cell-1", NotebookID: "nb-1"}, nil
		}
		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}

		_, err := svc.UpdateCell(ctx, "bob", false, "cell-1", domain.UpdateCellRequest{Content: ptrStr("nope")})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === DeleteCell ===

func TestNotebookService_DeleteCell(t *testing.T) {
	t.Run("owner can delete", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetCellFn = func(_ context.Context, _ string) (*domain.Cell, error) {
			return &domain.Cell{ID: "cell-1", NotebookID: "nb-1"}, nil
		}
		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}
		repo.DeleteCellFn = func(_ context.Context, _ string) error { return nil }

		err := svc.DeleteCell(ctx, "alice", false, "cell-1")
		require.NoError(t, err)
	})

	t.Run("non-owner non-admin denied", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetCellFn = func(_ context.Context, _ string) (*domain.Cell, error) {
			return &domain.Cell{ID: "cell-1", NotebookID: "nb-1"}, nil
		}
		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}

		err := svc.DeleteCell(ctx, "bob", false, "cell-1")
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === ReorderCells ===

func TestNotebookService_ReorderCells(t *testing.T) {
	t.Run("owner can reorder", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}
		repo.ReorderCellsFn = func(_ context.Context, _ string, _ []string) error { return nil }
		repo.ListCellsFn = func(_ context.Context, _ string) ([]domain.Cell, error) {
			return []domain.Cell{
				{ID: "cell-2", Position: 0},
				{ID: "cell-1", Position: 1},
			}, nil
		}

		cells, err := svc.ReorderCells(ctx, "alice", false, "nb-1", domain.ReorderCellsRequest{
			CellIDs: []string{"cell-2", "cell-1"},
		})
		require.NoError(t, err)
		assert.Len(t, cells, 2)
	})

	t.Run("non-owner non-admin denied", func(t *testing.T) {
		svc, repo, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
		}

		_, err := svc.ReorderCells(ctx, "bob", false, "nb-1", domain.ReorderCellsRequest{
			CellIDs: []string{"cell-1"},
		})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === ListNotebooks ===

func TestNotebookService_ListNotebooks(t *testing.T) {
	svc, repo, _ := setupNotebookService(t)
	ctx := context.Background()

	repo.ListNotebooksFn = func(_ context.Context, owner *string, _ domain.PageRequest) ([]domain.Notebook, int64, error) {
		return []domain.Notebook{{ID: "nb-1", Name: "NB"}}, 1, nil
	}

	nbs, total, err := svc.ListNotebooks(ctx, nil, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, nbs, 1)
}
