package model

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"duck-demo/internal/domain"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type constructorMacroRepoStub struct{}

func (constructorMacroRepoStub) Create(context.Context, *domain.Macro) (*domain.Macro, error) {
	panic("unexpected call")
}
func (constructorMacroRepoStub) GetByName(context.Context, string) (*domain.Macro, error) {
	panic("unexpected call")
}
func (constructorMacroRepoStub) List(context.Context, domain.PageRequest) ([]domain.Macro, int64, error) {
	panic("unexpected call")
}
func (constructorMacroRepoStub) Update(context.Context, string, domain.UpdateMacroRequest) (*domain.Macro, error) {
	panic("unexpected call")
}
func (constructorMacroRepoStub) Delete(context.Context, string) error { panic("unexpected call") }
func (constructorMacroRepoStub) ListAll(context.Context) ([]domain.Macro, error) {
	return []domain.Macro{}, nil
}
func (constructorMacroRepoStub) ListRevisions(context.Context, string) ([]domain.MacroRevision, error) {
	panic("unexpected call")
}
func (constructorMacroRepoStub) GetRevisionByVersion(context.Context, string, int) (*domain.MacroRevision, error) {
	panic("unexpected call")
}

func TestNewService_AssignsConstructorDependencies(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	engine := passthroughSessionEngine{}
	notebooks := promotionNotebookProviderStub{}
	notebookLinks := promotionLinkRepoStub{}
	macros := constructorMacroRepoStub{}

	svc := NewService(ServiceDeps{
		Macros:        macros,
		Notebooks:     notebooks,
		NotebookLinks: notebookLinks,
		Engine:        engine,
		Logger:        logger,
	})

	require.NotNil(t, svc)
	assert.Same(t, logger, svc.logger)
	assert.NotNil(t, svc.engine)
	assert.NotNil(t, svc.macros)
	assert.NotNil(t, svc.notebooks)
	assert.NotNil(t, svc.notebookLinks)
}

func TestNewService_PromoteNotebookRequiresNotebookProvider(t *testing.T) {
	svc := NewService(ServiceDeps{
		Models: promotionModelRepoStub{},
		Audit:  promotionAuditRepoStub{},
		Logger: slog.New(slog.DiscardHandler),
	})

	_, err := svc.PromoteNotebook(context.Background(), "alice", domain.PromoteNotebookRequest{
		NotebookID:      "nb-1",
		OutputCellID:    "cell-out",
		ProjectName:     "analytics",
		Name:            "orders",
		Materialization: domain.MaterializationTable,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "notebook provider not configured")
}

func TestNewService_VerifyMacrosLoadableWithoutMacroRepo(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	svc := NewService(ServiceDeps{
		DuckDB: db,
		Logger: slog.New(slog.DiscardHandler),
	})

	require.NoError(t, svc.verifyMacrosLoadable(context.Background(), "alice"))
}
