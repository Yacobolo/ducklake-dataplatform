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

type stubContextInvalidator struct {
	notebookIDs []string
	err         error
}

func (s *stubContextInvalidator) InvalidateNotebook(_ context.Context, notebookID string) error {
	s.notebookIDs = append(s.notebookIDs, notebookID)
	return s.err
}

func setupNotebookService(t *testing.T) (*Service, *testutil.MockNotebookRepo, *testutil.MockFolderRepo, *testutil.MockAuditRepo) {
	t.Helper()
	repo := &testutil.MockNotebookRepo{}
	folders := &testutil.MockFolderRepo{}
	folderShares := &testutil.MockFolderShareRepo{}
	notebookShares := &testutil.MockNotebookShareRepo{}
	audit := &testutil.MockAuditRepo{}
	svc := New(repo, audit)
	svc.SetFolderRepository(folders)
	svc.SetShareRepositories(folderShares, notebookShares)
	svc.SetWorkspaceRepository(newStubWorkspaceRepo(domain.FolderShareRoleManager))
	folders.ListAncestorsFn = func(_ context.Context, folderID string) ([]domain.Folder, error) {
		if folderID == "" {
			return nil, nil
		}
		if folders.GetByIDFn != nil {
			folder, err := folders.GetByID(context.Background(), folderID)
			if err == nil && folder != nil {
				return []domain.Folder{*folder}, nil
			}
		}
		return []domain.Folder{{ID: folderID, WorkspaceID: testWorkspaceID, Owner: "alice"}}, nil
	}
	return svc, repo, folders, audit
}

func setupNotebookServiceWithShares(t *testing.T) (
	*Service,
	*testutil.MockNotebookRepo,
	*testutil.MockFolderRepo,
	*testutil.MockFolderShareRepo,
	*testutil.MockNotebookShareRepo,
	*testutil.MockAuditRepo,
) {
	t.Helper()
	repo := &testutil.MockNotebookRepo{}
	folders := &testutil.MockFolderRepo{}
	folderShares := &testutil.MockFolderShareRepo{}
	notebookShares := &testutil.MockNotebookShareRepo{}
	audit := &testutil.MockAuditRepo{}
	svc := New(repo, audit)
	svc.SetFolderRepository(folders)
	svc.SetShareRepositories(folderShares, notebookShares)
	svc.SetWorkspaceRepository(newStubWorkspaceRepo(domain.FolderShareRoleManager))
	folders.ListAncestorsFn = func(_ context.Context, folderID string) ([]domain.Folder, error) {
		if folderID == "" {
			return nil, nil
		}
		if folders.GetByIDFn != nil {
			folder, err := folders.GetByID(context.Background(), folderID)
			if err == nil && folder != nil {
				return []domain.Folder{*folder}, nil
			}
		}
		return []domain.Folder{{ID: folderID, WorkspaceID: testWorkspaceID, Owner: "alice"}}, nil
	}
	return svc, repo, folders, folderShares, notebookShares, audit
}

// === CreateNotebook ===

func TestNotebookService_CreateNotebook(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		svc, repo, folders, audit := setupNotebookService(t)
		ctx := context.Background()

		folders.EnsurePersonalWorkspaceRootFn = func(_ context.Context, owner string) (*domain.Folder, error) {
			require.Equal(t, "alice", owner)
			return &domain.Folder{ID: "folder-1", WorkspaceID: testWorkspaceID, Owner: owner}, nil
		}
		folders.GetByIDFn = func(_ context.Context, id string) (*domain.Folder, error) {
			require.Equal(t, "folder-1", id)
			return &domain.Folder{ID: id, WorkspaceID: testWorkspaceID, Owner: "alice"}, nil
		}
		repo.CreateNotebookFn = func(_ context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
			assert.Equal(t, "Test NB", nb.Name)
			assert.Equal(t, "alice", nb.Owner)
			assert.Equal(t, "folder-1", nb.FolderID)
			return &domain.Notebook{ID: "nb-1", FolderID: nb.FolderID, Name: nb.Name, Owner: nb.Owner}, nil
		}

		result, err := svc.CreateNotebook(ctx, "alice", domain.CreateNotebookRequest{Name: "Test NB"})
		require.NoError(t, err)
		assert.Equal(t, "nb-1", result.ID)
		assert.True(t, audit.HasAction("CREATE_NOTEBOOK"))
	})

	t.Run("validation error empty name", func(t *testing.T) {
		svc, _, _, _ := setupNotebookService(t)
		ctx := context.Background()

		_, err := svc.CreateNotebook(ctx, "alice", domain.CreateNotebookRequest{Name: ""})
		require.Error(t, err)
		var validationErr *domain.ValidationError
		assert.ErrorAs(t, err, &validationErr)
	})

	t.Run("source does not create synthetic initial cell", func(t *testing.T) {
		svc, repo, folders, _ := setupNotebookService(t)
		ctx := context.Background()
		source := "sql"

		folders.EnsurePersonalWorkspaceRootFn = func(_ context.Context, owner string) (*domain.Folder, error) {
			return &domain.Folder{ID: "folder-2", WorkspaceID: testWorkspaceID, Owner: owner}, nil
		}
		folders.GetByIDFn = func(_ context.Context, id string) (*domain.Folder, error) {
			require.Equal(t, "folder-2", id)
			return &domain.Folder{ID: id, WorkspaceID: testWorkspaceID, Owner: "alice"}, nil
		}
		repo.CreateNotebookFn = func(_ context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-2", FolderID: nb.FolderID, Name: nb.Name, Owner: nb.Owner}, nil
		}
		repo.CreateCellFn = func(_ context.Context, _ *domain.Cell) (*domain.Cell, error) {
			t.Fatal("CreateCell should not be called for notebook source metadata")
			return nil, nil
		}

		_, err := svc.CreateNotebook(ctx, "alice", domain.CreateNotebookRequest{Name: "No Seed", Source: &source})
		require.NoError(t, err)
	})
}

// === GetNotebook ===

func TestNotebookService_GetNotebook(t *testing.T) {
	t.Run("success with cells", func(t *testing.T) {
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return nil, domain.ErrNotFound("not found")
		}

		_, _, err := svc.GetNotebook(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("non-owner denied on secure read", func(t *testing.T) {
		svc, repo, _, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}

		_, _, err := svc.GetNotebookForPrincipal(ctx, "bob", false, "nb-1")
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})

	t.Run("direct notebook share grants secure read", func(t *testing.T) {
		svc, repo, _, _, notebookShares, _ := setupNotebookServiceWithShares(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}
		repo.ListCellsFn = func(_ context.Context, notebookID string) ([]domain.Cell, error) {
			return []domain.Cell{{ID: "cell-1", NotebookID: notebookID, CellType: domain.CellTypeSQL, Content: "SELECT 1"}}, nil
		}
		notebookShares.ListByPrincipalFn = func(_ context.Context, principalName string) ([]domain.NotebookShare, error) {
			require.Equal(t, "bob", principalName)
			return []domain.NotebookShare{{NotebookID: "nb-1", PrincipalName: principalName, Role: domain.FolderShareRoleViewer}}, nil
		}

		nb, cells, err := svc.GetNotebookForPrincipal(ctx, "bob", false, "nb-1")
		require.NoError(t, err)
		assert.Equal(t, "nb-1", nb.ID)
		assert.Len(t, cells, 1)
	})
}

// === UpdateNotebook ===

func TestNotebookService_UpdateNotebook(t *testing.T) {
	t.Run("owner can update", func(t *testing.T) {
		svc, repo, _, audit := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Name: "Old", Owner: "alice"}, nil
		}

		_, err := svc.UpdateNotebook(ctx, "bob", false, "nb-1", domain.UpdateNotebookRequest{Name: ptrStr("Denied")})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})

	t.Run("shared editor can update notebook metadata", func(t *testing.T) {
		svc, repo, _, _, notebookShares, _ := setupNotebookServiceWithShares(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", Name: "Old", Owner: "alice"}, nil
		}
		repo.UpdateNotebookFn = func(_ context.Context, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: *req.Name, Owner: "alice"}, nil
		}
		notebookShares.ListByPrincipalFn = func(_ context.Context, principalName string) ([]domain.NotebookShare, error) {
			require.Equal(t, "bob", principalName)
			return []domain.NotebookShare{{NotebookID: "nb-1", PrincipalName: principalName, Role: domain.FolderShareRoleEditor}}, nil
		}

		result, err := svc.UpdateNotebook(ctx, "bob", false, "nb-1", domain.UpdateNotebookRequest{Name: ptrStr("New")})
		require.NoError(t, err)
		assert.Equal(t, "New", result.Name)
	})
}

// === DeleteNotebook ===

func TestNotebookService_DeleteNotebook(t *testing.T) {
	t.Run("owner can delete", func(t *testing.T) {
		svc, repo, _, audit := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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

func TestNotebookService_ShareNotebook(t *testing.T) {
	svc, repo, _, _, notebookShares, audit := setupNotebookServiceWithShares(t)
	ctx := context.Background()

	repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
		return &domain.Notebook{ID: "nb-1", Owner: "alice"}, nil
	}
	notebookShares.UpsertFn = func(_ context.Context, share *domain.NotebookShare) (*domain.NotebookShare, error) {
		require.Equal(t, "nb-1", share.NotebookID)
		require.Equal(t, "bob", share.PrincipalName)
		require.Equal(t, domain.FolderShareRoleViewer, share.Role)
		return share, nil
	}

	share, err := svc.ShareNotebook(ctx, "alice", false, "nb-1", domain.NotebookShare{PrincipalName: "bob"})
	require.NoError(t, err)
	assert.Equal(t, "bob", share.PrincipalName)
	assert.True(t, audit.HasAction("SHARE_NOTEBOOK"))
}

// === MoveNotebook ===

func TestNotebookService_MoveNotebook(t *testing.T) {
	t.Run("moves notebook into project-bound git folder and invalidates context", func(t *testing.T) {
		svc, repo, folders, audit := setupNotebookService(t)
		invalidator := &stubContextInvalidator{}
		svc.SetContextInvalidator(invalidator)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{
				ID:       "nb-1",
				FolderID: "folder-source",
				Name:     "Finance Report",
				Owner:    "alice",
			}, nil
		}
		repo.ListNotebooksFn = func(_ context.Context, _ *string, _ domain.PageRequest) ([]domain.Notebook, int64, error) {
			return nil, 0, nil
		}
		repo.UpdateNotebookSyncFn = func(_ context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
			require.Equal(t, "folder-target", nb.FolderID)
			require.Equal(t, "repo-1", *nb.GitRepoID)
			require.Equal(t, "analytics/quarterly/finance-report.yaml", *nb.GitPath)
			return nb, nil
		}
		folders.GetByIDFn = func(_ context.Context, id string) (*domain.Folder, error) {
			switch id {
			case "folder-source":
				return &domain.Folder{ID: "folder-source", WorkspaceID: testWorkspaceID, Name: "Home", Owner: "alice"}, nil
			case "folder-target":
				return &domain.Folder{ID: "folder-target", WorkspaceID: testWorkspaceID, Name: "Quarterly", Owner: "alice"}, nil
			default:
				t.Fatalf("unexpected folder lookup %q", id)
				return nil, nil
			}
		}
		folders.ListAncestorsFn = func(_ context.Context, folderID string) ([]domain.Folder, error) {
			switch folderID {
			case "folder-source":
				return []domain.Folder{
					{ID: "folder-source", WorkspaceID: testWorkspaceID, Name: "Home"},
					{ID: "source-parent", WorkspaceID: testWorkspaceID, DefaultProjectID: ptrStr("project-dev")},
				}, nil
			case "folder-target":
				return []domain.Folder{
					{ID: "folder-target", WorkspaceID: testWorkspaceID, Name: "Quarterly"},
					{ID: "repo-root", WorkspaceID: testWorkspaceID, Name: "Analytics", GitRepoID: ptrStr("repo-1"), GitRootPath: ptrStr("analytics"), DefaultProjectID: ptrStr("project-prod")},
				}, nil
			default:
				t.Fatalf("unexpected folder lookup %q", folderID)
				return nil, nil
			}
		}

		moved, err := svc.MoveNotebook(ctx, "alice", false, "nb-1", domain.MoveNotebookRequest{
			FolderID:             "folder-target",
			ConfirmContextChange: true,
		})
		require.NoError(t, err)
		assert.Equal(t, "folder-target", moved.FolderID)
		assert.Equal(t, []string{"nb-1"}, invalidator.notebookIDs)
		assert.True(t, audit.HasAction("MOVE_NOTEBOOK"))
	})

	t.Run("blocks cross repo moves", func(t *testing.T) {
		svc, repo, folders, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", FolderID: "folder-source", Name: "Notebook", Owner: "alice"}, nil
		}
		folders.GetByIDFn = func(_ context.Context, _ string) (*domain.Folder, error) {
			return &domain.Folder{ID: "folder-target", WorkspaceID: testWorkspaceID, Owner: "alice"}, nil
		}
		folders.ListAncestorsFn = func(_ context.Context, folderID string) ([]domain.Folder, error) {
			switch folderID {
			case "folder-source":
				return []domain.Folder{{ID: "repo-a", WorkspaceID: testWorkspaceID, GitRepoID: ptrStr("repo-a")}}, nil
			case "folder-target":
				return []domain.Folder{{ID: "repo-b", WorkspaceID: testWorkspaceID, GitRepoID: ptrStr("repo-b")}}, nil
			default:
				return nil, domain.ErrNotFound("folder not found")
			}
		}

		_, err := svc.MoveNotebook(ctx, "alice", false, "nb-1", domain.MoveNotebookRequest{FolderID: "folder-target"})
		require.Error(t, err)
		var conflictErr *domain.ConflictError
		assert.ErrorAs(t, err, &conflictErr)
	})

	t.Run("requires confirmation when leaving git", func(t *testing.T) {
		svc, repo, folders, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: "nb-1", FolderID: "folder-source", Name: "Notebook", Owner: "alice"}, nil
		}
		folders.GetByIDFn = func(_ context.Context, _ string) (*domain.Folder, error) {
			return &domain.Folder{ID: "folder-target", WorkspaceID: testWorkspaceID, Owner: "alice"}, nil
		}
		folders.ListAncestorsFn = func(_ context.Context, folderID string) ([]domain.Folder, error) {
			switch folderID {
			case "folder-source":
				return []domain.Folder{{ID: "repo-a", WorkspaceID: testWorkspaceID, GitRepoID: ptrStr("repo-a")}}, nil
			case "folder-target":
				return []domain.Folder{{ID: "folder-target", WorkspaceID: testWorkspaceID}}, nil
			default:
				return nil, domain.ErrNotFound("folder not found")
			}
		}

		_, err := svc.MoveNotebook(ctx, "alice", false, "nb-1", domain.MoveNotebookRequest{FolderID: "folder-target"})
		require.Error(t, err)
		var validationErr *domain.ValidationError
		assert.ErrorAs(t, err, &validationErr)
	})
}

// === DuplicateNotebook ===

func TestNotebookService_DuplicateNotebook(t *testing.T) {
	t.Run("copies cells into destination folder", func(t *testing.T) {
		svc, repo, folders, audit := setupNotebookService(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return &domain.Notebook{
				ID:                    "nb-source",
				FolderID:              "folder-source",
				Name:                  "Finance Report",
				Description:           ptrStr("Quarterly KPIs"),
				Owner:                 "alice",
				ProjectOverrideID:     ptrStr("project-debug"),
				EnvironmentOverrideID: ptrStr("env-debug"),
			}, nil
		}
		repo.ListNotebooksFn = func(_ context.Context, _ *string, _ domain.PageRequest) ([]domain.Notebook, int64, error) {
			return nil, 0, nil
		}
		repo.CreateNotebookFn = func(_ context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
			require.Equal(t, "folder-target", nb.FolderID)
			require.Equal(t, "analytics/quarterly/finance-report-copy.yaml", *nb.GitPath)
			return &domain.Notebook{
				ID:                    "nb-copy",
				FolderID:              nb.FolderID,
				Name:                  nb.Name,
				Description:           nb.Description,
				Owner:                 nb.Owner,
				GitRepoID:             nb.GitRepoID,
				GitPath:               nb.GitPath,
				ProjectOverrideID:     nb.ProjectOverrideID,
				EnvironmentOverrideID: nb.EnvironmentOverrideID,
			}, nil
		}
		repo.ListCellsFn = func(_ context.Context, notebookID string) ([]domain.Cell, error) {
			require.Equal(t, "nb-source", notebookID)
			return []domain.Cell{
				{ID: "cell-1", NotebookID: notebookID, CellType: domain.CellTypeSQL, Content: "SELECT 1", Position: 0},
				{ID: "cell-2", NotebookID: notebookID, CellType: domain.CellTypeMarkdown, Content: "# Notes", Position: 1},
			}, nil
		}
		createdCells := make([]domain.Cell, 0, 2)
		repo.CreateCellFn = func(_ context.Context, cell *domain.Cell) (*domain.Cell, error) {
			createdCells = append(createdCells, *cell)
			return cell, nil
		}
		folders.GetByIDFn = func(_ context.Context, id string) (*domain.Folder, error) {
			require.Equal(t, "folder-target", id)
			return &domain.Folder{ID: "folder-target", WorkspaceID: testWorkspaceID, Name: "Quarterly", Owner: "alice"}, nil
		}
		folders.ListAncestorsFn = func(_ context.Context, folderID string) ([]domain.Folder, error) {
			require.Equal(t, "folder-target", folderID)
			return []domain.Folder{
				{ID: "folder-target", WorkspaceID: testWorkspaceID, Name: "Quarterly"},
				{ID: "repo-root", WorkspaceID: testWorkspaceID, Name: "Analytics", GitRepoID: ptrStr("repo-1"), GitRootPath: ptrStr("analytics")},
			}, nil
		}

		duplicated, err := svc.DuplicateNotebook(ctx, "alice", false, "nb-source", domain.DuplicateNotebookRequest{
			FolderID: "folder-target",
			Name:     ptrStr("Finance Report Copy"),
		})
		require.NoError(t, err)
		assert.Equal(t, "nb-copy", duplicated.ID)
		assert.Len(t, createdCells, 2)
		assert.Equal(t, "nb-copy", createdCells[0].NotebookID)
		assert.True(t, audit.HasAction("DUPLICATE_NOTEBOOK"))
	})
}

// === CreateCell ===

func TestNotebookService_CreateCell(t *testing.T) {
	t.Run("success with auto position", func(t *testing.T) {
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, _, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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
		svc, repo, _, _ := setupNotebookService(t)
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
	t.Run("base list", func(t *testing.T) {
		svc, repo, _, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.ListNotebooksFn = func(_ context.Context, owner *string, _ domain.PageRequest) ([]domain.Notebook, int64, error) {
			return []domain.Notebook{{ID: "nb-1", Name: "NB"}}, 1, nil
		}

		nbs, total, err := svc.ListNotebooks(ctx, nil, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, nbs, 1)
	})

	t.Run("non-admin list is scoped to caller", func(t *testing.T) {
		svc, repo, _, _ := setupNotebookService(t)
		ctx := context.Background()

		repo.ListNotebooksFn = func(_ context.Context, owner *string, _ domain.PageRequest) ([]domain.Notebook, int64, error) {
			require.Nil(t, owner)
			return []domain.Notebook{
				{ID: "nb-1", Name: "Mine", Owner: "alice"},
				{ID: "nb-2", Name: "Other", Owner: "bob"},
			}, 2, nil
		}

		nbs, total, err := svc.ListNotebooksForPrincipal(ctx, "alice", false, nil, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, nbs, 1)
		assert.Equal(t, "nb-1", nbs[0].ID)
	})

	t.Run("non-admin can request another owner when notebook is shared", func(t *testing.T) {
		svc, repo, _, _, notebookShares, _ := setupNotebookServiceWithShares(t)
		ctx := context.Background()
		owner := "bob"

		repo.ListNotebooksFn = func(_ context.Context, currentOwner *string, _ domain.PageRequest) ([]domain.Notebook, int64, error) {
			require.Nil(t, currentOwner)
			return []domain.Notebook{{ID: "nb-shared", Name: "Shared", Owner: "bob"}}, 1, nil
		}
		notebookShares.ListByPrincipalFn = func(_ context.Context, principalName string) ([]domain.NotebookShare, error) {
			require.Equal(t, "alice", principalName)
			return []domain.NotebookShare{{NotebookID: "nb-shared", PrincipalName: principalName, Role: domain.FolderShareRoleViewer}}, nil
		}

		nbs, total, err := svc.ListNotebooksForPrincipal(ctx, "alice", false, &owner, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		require.Len(t, nbs, 1)
		assert.Equal(t, "nb-shared", nbs[0].ID)
	})
}
