package model

import (
	"context"
	"log/slog"
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type promotionModelRepoStub struct {
	listAllFn                func(context.Context) ([]domain.Model, error)
	getByNameFn              func(context.Context, string, string) (*domain.Model, error)
	updateFn                 func(context.Context, string, domain.UpdateModelRequest) (*domain.Model, error)
	updateDependenciesFn     func(context.Context, string, []string) error
	createWithNotebookLinkFn func(context.Context, *domain.Model, string, string) (*domain.Model, error)
}

func (s promotionModelRepoStub) Create(context.Context, *domain.Model) (*domain.Model, error) {
	panic("unexpected call")
}

func (s promotionModelRepoStub) CreateWithNotebookLink(ctx context.Context, m *domain.Model, notebookID, outputCellID string) (*domain.Model, error) {
	if s.createWithNotebookLinkFn != nil {
		return s.createWithNotebookLinkFn(ctx, m, notebookID, outputCellID)
	}
	panic("unexpected call")
}

func (s promotionModelRepoStub) GetByID(context.Context, string) (*domain.Model, error) {
	panic("unexpected call")
}

func (s promotionModelRepoStub) GetByName(ctx context.Context, projectName, name string) (*domain.Model, error) {
	if s.getByNameFn != nil {
		return s.getByNameFn(ctx, projectName, name)
	}
	return nil, domain.ErrNotFound("not found")
}

func (s promotionModelRepoStub) List(context.Context, *string, domain.PageRequest) ([]domain.Model, int64, error) {
	panic("unexpected call")
}

func (s promotionModelRepoStub) Update(ctx context.Context, id string, req domain.UpdateModelRequest) (*domain.Model, error) {
	if s.updateFn != nil {
		return s.updateFn(ctx, id, req)
	}
	panic("unexpected call")
}

func (s promotionModelRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}

func (s promotionModelRepoStub) ListAll(ctx context.Context) ([]domain.Model, error) {
	if s.listAllFn != nil {
		return s.listAllFn(ctx)
	}
	return []domain.Model{}, nil
}

func (s promotionModelRepoStub) UpdateDependencies(ctx context.Context, id string, deps []string) error {
	if s.updateDependenciesFn != nil {
		return s.updateDependenciesFn(ctx, id, deps)
	}
	panic("unexpected call")
}

type promotionNotebookProviderStub struct {
	compileOutputCellSQLFn func(context.Context, string, string) (string, error)
}

func (s promotionNotebookProviderStub) GetSQLBlocks(context.Context, string) ([]string, error) {
	return nil, domain.ErrNotImplemented("unused")
}

func (s promotionNotebookProviderStub) GetExecutableCells(context.Context, string) ([]domain.NotebookExecutableCell, error) {
	return nil, domain.ErrNotImplemented("unused")
}

func (s promotionNotebookProviderStub) GetSQLBlockByCellID(context.Context, string, string) (string, error) {
	return "", domain.ErrNotImplemented("unused")
}

func (s promotionNotebookProviderStub) CompileOutputCellSQL(ctx context.Context, notebookID, outputCellID string) (string, error) {
	if s.compileOutputCellSQLFn != nil {
		return s.compileOutputCellSQLFn(ctx, notebookID, outputCellID)
	}
	return "", domain.ErrNotImplemented("unused")
}

func (s promotionNotebookProviderStub) ListCells(context.Context, string) ([]domain.Cell, error) {
	return nil, domain.ErrNotImplemented("unused")
}

type promotionLinkRepoStub struct {
	upsertFn func(context.Context, *domain.NotebookModelLink) error
}

func (s promotionLinkRepoStub) Upsert(ctx context.Context, link *domain.NotebookModelLink) error {
	if s.upsertFn != nil {
		return s.upsertFn(ctx, link)
	}
	panic("unexpected call")
}

func (s promotionLinkRepoStub) GetByNotebookID(context.Context, string) (*domain.NotebookModelLink, error) {
	panic("unexpected call")
}

func (s promotionLinkRepoStub) GetByModelID(context.Context, string) (*domain.NotebookModelLink, error) {
	panic("unexpected call")
}

func (s promotionLinkRepoStub) DeleteByNotebookID(context.Context, string) error {
	panic("unexpected call")
}

type promotionAuditRepoStub struct{}

func (s promotionAuditRepoStub) Insert(context.Context, *domain.AuditEntry) error { return nil }

func (s promotionAuditRepoStub) List(context.Context, domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	panic("unexpected call")
}

func TestPromoteNotebook_CreatesWhenModelDoesNotExist(t *testing.T) {
	ctx := context.Background()
	var created *domain.Model
	var createNotebookID string
	var createOutputCellID string

	svc := &Service{
		models: promotionModelRepoStub{
			getByNameFn: func(context.Context, string, string) (*domain.Model, error) {
				return nil, domain.ErrNotFound("missing")
			},
			createWithNotebookLinkFn: func(_ context.Context, m *domain.Model, notebookID, outputCellID string) (*domain.Model, error) {
				created = m
				createNotebookID = notebookID
				createOutputCellID = outputCellID
				return &domain.Model{
					ID:              "m-1",
					ProjectName:     m.ProjectName,
					Name:            m.Name,
					SQL:             m.SQL,
					Materialization: m.Materialization,
					DependsOn:       m.DependsOn,
				}, nil
			},
		},
		notebooks: promotionNotebookProviderStub{compileOutputCellSQLFn: func(_ context.Context, notebookID, outputCellID string) (string, error) {
			require.Equal(t, "nb-1", notebookID)
			require.Equal(t, "cell-out", outputCellID)
			return "SELECT 1 AS id", nil
		}},
		audit:  promotionAuditRepoStub{},
		logger: slog.New(slog.DiscardHandler),
	}

	got, err := svc.PromoteNotebook(ctx, "alice", domain.PromoteNotebookRequest{
		NotebookID:      "nb-1",
		OutputCellID:    "cell-out",
		ProjectName:     "analytics",
		Name:            "orders",
		Materialization: domain.MaterializationTable,
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, created)
	assert.Equal(t, "nb-1", createNotebookID)
	assert.Equal(t, "cell-out", createOutputCellID)
	assert.Equal(t, "analytics", created.ProjectName)
	assert.Equal(t, "orders", created.Name)
	assert.Equal(t, "SELECT 1 AS id", created.SQL)
}

func TestPromoteNotebook_UpdatesWhenModelExists(t *testing.T) {
	ctx := context.Background()
	existing := &domain.Model{ID: "m-1", ProjectName: "analytics", Name: "orders", Materialization: domain.MaterializationView}
	var updatedReq domain.UpdateModelRequest
	var updatedDeps []string
	var upsertLink *domain.NotebookModelLink

	svc := &Service{
		models: promotionModelRepoStub{
			listAllFn: func(context.Context) ([]domain.Model, error) {
				return []domain.Model{{ID: "m-base", ProjectName: "analytics", Name: "base"}}, nil
			},
			getByNameFn: func(context.Context, string, string) (*domain.Model, error) {
				return existing, nil
			},
			updateFn: func(_ context.Context, id string, req domain.UpdateModelRequest) (*domain.Model, error) {
				require.Equal(t, existing.ID, id)
				updatedReq = req
				return &domain.Model{
					ID:              existing.ID,
					ProjectName:     existing.ProjectName,
					Name:            existing.Name,
					Materialization: *req.Materialization,
					SQL:             *req.SQL,
				}, nil
			},
			updateDependenciesFn: func(_ context.Context, _ string, deps []string) error {
				updatedDeps = deps
				return nil
			},
		},
		notebooks: promotionNotebookProviderStub{compileOutputCellSQLFn: func(context.Context, string, string) (string, error) {
			return "SELECT * FROM base", nil
		}},
		notebookLinks: promotionLinkRepoStub{upsertFn: func(_ context.Context, link *domain.NotebookModelLink) error {
			upsertLink = link
			return nil
		}},
		audit:  promotionAuditRepoStub{},
		logger: slog.New(slog.DiscardHandler),
	}

	got, err := svc.PromoteNotebook(ctx, "alice", domain.PromoteNotebookRequest{
		NotebookID:      "nb-1",
		OutputCellID:    "cell-out",
		ProjectName:     "analytics",
		Name:            "orders",
		Materialization: domain.MaterializationTable,
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, updatedReq.SQL)
	require.NotNil(t, updatedReq.Materialization)
	assert.Equal(t, "SELECT * FROM base", *updatedReq.SQL)
	assert.Equal(t, domain.MaterializationTable, *updatedReq.Materialization)
	assert.Contains(t, updatedDeps, "analytics.base")
	require.NotNil(t, upsertLink)
	assert.Equal(t, "nb-1", upsertLink.NotebookID)
	assert.Equal(t, "m-1", upsertLink.ModelID)
	assert.Equal(t, "cell-out", upsertLink.OutputCellID)
}

func TestPromoteNotebook_UpdateRequiresLinkRepository(t *testing.T) {
	svc := &Service{
		models: promotionModelRepoStub{
			getByNameFn: func(context.Context, string, string) (*domain.Model, error) {
				return &domain.Model{ID: "m-1", ProjectName: "analytics", Name: "orders"}, nil
			},
			updateFn: func(_ context.Context, id string, req domain.UpdateModelRequest) (*domain.Model, error) {
				return &domain.Model{ID: id, ProjectName: "analytics", Name: "orders", SQL: *req.SQL, Materialization: *req.Materialization}, nil
			},
			updateDependenciesFn: func(context.Context, string, []string) error { return nil },
		},
		notebooks: promotionNotebookProviderStub{compileOutputCellSQLFn: func(context.Context, string, string) (string, error) {
			return "SELECT 1", nil
		}},
		audit:  promotionAuditRepoStub{},
		logger: slog.New(slog.DiscardHandler),
	}

	_, err := svc.PromoteNotebook(context.Background(), "alice", domain.PromoteNotebookRequest{
		NotebookID:      "nb-1",
		OutputCellID:    "cell-out",
		ProjectName:     "analytics",
		Name:            "orders",
		Materialization: domain.MaterializationTable,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "notebook-model link repository not configured")
}
