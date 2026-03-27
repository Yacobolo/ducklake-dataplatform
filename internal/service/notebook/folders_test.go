package notebook

import (
	"context"
	"testing"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupFolderService(t *testing.T) (*FolderService, *testutil.MockFolderRepo, *testutil.MockAuditRepo) {
	t.Helper()
	repo := &testutil.MockFolderRepo{}
	folderShares := &testutil.MockFolderShareRepo{}
	audit := &testutil.MockAuditRepo{}
	svc := NewFolderService(repo, audit)
	svc.SetShareRepository(folderShares)
	return svc, repo, audit
}

func setupFolderServiceWithShares(t *testing.T) (*FolderService, *testutil.MockFolderRepo, *testutil.MockFolderShareRepo, *testutil.MockAuditRepo) {
	t.Helper()
	repo := &testutil.MockFolderRepo{}
	folderShares := &testutil.MockFolderShareRepo{}
	audit := &testutil.MockAuditRepo{}
	svc := NewFolderService(repo, audit)
	svc.SetShareRepository(folderShares)
	return svc, repo, folderShares, audit
}

func setupFolderServiceWithInvalidation(t *testing.T) (*FolderService, *testutil.MockFolderRepo, *testutil.MockNotebookRepo, *testutil.MockOrchestrationEventRepo, *testutil.MockAuditRepo) {
	t.Helper()
	repo := &testutil.MockFolderRepo{}
	notebooks := &testutil.MockNotebookRepo{}
	events := &testutil.MockOrchestrationEventRepo{}
	audit := &testutil.MockAuditRepo{}
	svc := NewFolderService(repo, audit)
	svc.SetContextInvalidation(notebooks, events)
	return svc, repo, notebooks, events, audit
}

func TestFolderService_CreateFolder(t *testing.T) {
	svc, repo, audit := setupFolderService(t)
	ctx := context.Background()

	parentID := "folder-parent"
	repo.GetByIDFn = func(_ context.Context, id string) (*domain.Folder, error) {
		require.Equal(t, parentID, id)
		return &domain.Folder{ID: id, Owner: "alice"}, nil
	}
	repo.CreateFn = func(_ context.Context, folder *domain.Folder) (*domain.Folder, error) {
		require.Equal(t, "Finance", folder.Name)
		require.Equal(t, "alice", folder.Owner)
		require.Equal(t, parentID, *folder.ParentFolderID)
		return folder, nil
	}

	folder, err := svc.CreateFolder(ctx, "alice", domain.CreateFolderRequest{
		Name:           "Finance",
		ParentFolderID: &parentID,
	})
	require.NoError(t, err)
	assert.Equal(t, "Finance", folder.Name)
	assert.True(t, audit.HasAction("CREATE_FOLDER"))
}

func TestFolderService_UpdateFolder(t *testing.T) {
	svc, repo, audit := setupFolderService(t)
	ctx := context.Background()

	repo.GetByIDFn = func(_ context.Context, id string) (*domain.Folder, error) {
		return &domain.Folder{ID: id, Name: "Finance", Owner: "alice"}, nil
	}
	repo.UpdateFn = func(_ context.Context, id string, req domain.UpdateFolderRequest) (*domain.Folder, error) {
		require.Equal(t, "folder-1", id)
		require.Equal(t, "Finance Core", *req.Name)
		return &domain.Folder{ID: id, Name: *req.Name, Owner: "alice"}, nil
	}

	updated, err := svc.UpdateFolder(ctx, "alice", false, "folder-1", domain.UpdateFolderRequest{Name: ptrStr("Finance Core")})
	require.NoError(t, err)
	assert.Equal(t, "Finance Core", updated.Name)
	assert.True(t, audit.HasAction("UPDATE_FOLDER"))
}

func TestFolderService_DeleteFolderPersonalRootDenied(t *testing.T) {
	svc, repo, _ := setupFolderService(t)
	ctx := context.Background()
	role := domain.FolderSystemRolePersonalRoot

	repo.GetByIDFn = func(_ context.Context, id string) (*domain.Folder, error) {
		return &domain.Folder{ID: id, Owner: "alice", SystemRole: &role}, nil
	}

	err := svc.DeleteFolder(ctx, "alice", false, "folder-root")
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestFolderService_ListFoldersForPrincipal(t *testing.T) {
	svc, repo, _ := setupFolderService(t)
	ctx := context.Background()

	repo.ListAllFn = func(_ context.Context) ([]domain.Folder, error) {
		return []domain.Folder{{ID: "folder-1", Owner: "alice"}}, nil
	}

	items, err := svc.ListFoldersForPrincipal(ctx, "alice", false, nil)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "folder-1", items[0].ID)
}

func TestFolderService_ListFoldersForPrincipal_IncludesShared(t *testing.T) {
	svc, repo, folderShares, _ := setupFolderServiceWithShares(t)
	ctx := context.Background()

	repo.ListAllFn = func(_ context.Context) ([]domain.Folder, error) {
		return []domain.Folder{
			{ID: "folder-1", Owner: "bob"},
			{ID: "folder-2", Owner: "charlie"},
		}, nil
	}
	repo.ListAncestorsFn = func(_ context.Context, folderID string) ([]domain.Folder, error) {
		return []domain.Folder{{ID: folderID}}, nil
	}
	folderShares.ListByPrincipalFn = func(_ context.Context, principalName string) ([]domain.FolderShare, error) {
		require.Equal(t, "alice", principalName)
		return []domain.FolderShare{{FolderID: "folder-1", PrincipalName: principalName, Role: domain.FolderShareRoleViewer}}, nil
	}

	items, err := svc.ListFoldersForPrincipal(ctx, "alice", false, nil)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "folder-1", items[0].ID)
}

func TestFolderService_ShareFolder(t *testing.T) {
	svc, repo, folderShares, audit := setupFolderServiceWithShares(t)
	ctx := context.Background()

	repo.GetByIDFn = func(_ context.Context, id string) (*domain.Folder, error) {
		return &domain.Folder{ID: id, Owner: "alice"}, nil
	}
	folderShares.UpsertFn = func(_ context.Context, share *domain.FolderShare) (*domain.FolderShare, error) {
		require.Equal(t, "folder-1", share.FolderID)
		require.Equal(t, "bob", share.PrincipalName)
		require.Equal(t, domain.FolderShareRoleEditor, share.Role)
		return share, nil
	}

	share, err := svc.ShareFolder(ctx, "alice", false, "folder-1", domain.FolderShare{
		PrincipalName: "bob",
		Role:          domain.FolderShareRoleEditor,
	})
	require.NoError(t, err)
	assert.Equal(t, "bob", share.PrincipalName)
	assert.True(t, audit.HasAction("SHARE_FOLDER"))
}

func TestFolderService_UpdateFolder_EnqueuesSubtreeInvalidations(t *testing.T) {
	svc, repo, notebooks, events, audit := setupFolderServiceWithInvalidation(t)
	ctx := context.Background()

	repo.GetByIDFn = func(_ context.Context, id string) (*domain.Folder, error) {
		return &domain.Folder{ID: id, Owner: "alice", Path: "/root/folder-1", DefaultProjectID: ptrStr("project-a")}, nil
	}
	repo.UpdateFn = func(_ context.Context, id string, req domain.UpdateFolderRequest) (*domain.Folder, error) {
		return &domain.Folder{ID: id, Owner: "alice", Path: "/root/folder-1", DefaultProjectID: ptrStr("project-b")}, nil
	}
	repo.ListAllFn = func(_ context.Context) ([]domain.Folder, error) {
		return []domain.Folder{
			{ID: "folder-1", Path: "/root/folder-1"},
			{ID: "child-1", Path: "/root/folder-1/child-1"},
			{ID: "elsewhere", Path: "/root/elsewhere"},
		}, nil
	}
	notebooks.ListNotebooksFn = func(_ context.Context, owner *string, _ domain.PageRequest) ([]domain.Notebook, int64, error) {
		require.Nil(t, owner)
		return []domain.Notebook{
			{ID: "nb-root", FolderID: "folder-1"},
			{ID: "nb-child", FolderID: "child-1"},
			{ID: "nb-other", FolderID: "elsewhere"},
		}, 3, nil
	}
	enqueued := make([]string, 0, 2)
	events.EnqueueFn = func(_ context.Context, event *domain.OrchestrationEvent) (*domain.OrchestrationEvent, error) {
		require.Equal(t, domain.NotebookEventTypeInvalidateContext, event.EventType)
		notebookID, _ := event.PayloadJSON[domain.NotebookEventPayloadNotebookID].(string)
		enqueued = append(enqueued, notebookID)
		return event, nil
	}

	updated, err := svc.UpdateFolder(ctx, "alice", false, "folder-1", domain.UpdateFolderRequest{DefaultProjectID: ptrStr("project-b")})
	require.NoError(t, err)
	assert.Equal(t, "project-b", *updated.DefaultProjectID)
	assert.ElementsMatch(t, []string{"nb-root", "nb-child"}, enqueued)
	assert.True(t, audit.HasAction("UPDATE_FOLDER"))
}
