package model

import (
	"context"
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type notebookLinkRepoStub struct {
	getByModelIDFn func(ctx context.Context, modelID string) (*domain.NotebookModelLink, error)
}

func (s notebookLinkRepoStub) Upsert(context.Context, *domain.NotebookModelLink) error { return nil }
func (s notebookLinkRepoStub) GetByNotebookID(context.Context, string) (*domain.NotebookModelLink, error) {
	return nil, domain.ErrNotFound("not found")
}
func (s notebookLinkRepoStub) GetByModelID(ctx context.Context, modelID string) (*domain.NotebookModelLink, error) {
	if s.getByModelIDFn != nil {
		return s.getByModelIDFn(ctx, modelID)
	}
	return nil, domain.ErrNotFound("not found")
}
func (s notebookLinkRepoStub) DeleteByNotebookID(context.Context, string) error { return nil }

type notebookProviderStub struct {
	listCellsFn func(ctx context.Context, notebookID string) ([]domain.Cell, error)
}

func (s notebookProviderStub) GetSQLBlocks(context.Context, string) ([]string, error) {
	return nil, domain.ErrNotImplemented("unused")
}
func (s notebookProviderStub) GetExecutableCells(context.Context, string) ([]domain.NotebookExecutableCell, error) {
	return nil, domain.ErrNotImplemented("unused")
}
func (s notebookProviderStub) GetSQLBlockByCellID(context.Context, string, string) (string, error) {
	return "", domain.ErrNotImplemented("unused")
}
func (s notebookProviderStub) CompileOutputCellSQL(context.Context, string, string) (string, error) {
	return "", domain.ErrNotImplemented("unused")
}
func (s notebookProviderStub) ListCells(ctx context.Context, notebookID string) ([]domain.Cell, error) {
	if s.listCellsFn != nil {
		return s.listCellsFn(ctx, notebookID)
	}
	return []domain.Cell{}, nil
}

func TestGenerateTestSQL(t *testing.T) {
	tests := []struct {
		name    string
		test    domain.ModelTest
		schema  string
		model   string
		wantSQL string
		wantErr bool
	}{
		{
			name:    "not_null",
			test:    domain.ModelTest{TestType: "not_null", Column: "id"},
			schema:  "analytics",
			model:   "orders",
			wantSQL: `SELECT * FROM "analytics"."orders" WHERE "id" IS NULL LIMIT 1`,
		},
		{
			name:    "unique",
			test:    domain.ModelTest{TestType: "unique", Column: "email"},
			schema:  "analytics",
			model:   "users",
			wantSQL: `SELECT "email", COUNT(*) AS cnt FROM "analytics"."users" GROUP BY "email" HAVING cnt > 1 LIMIT 1`,
		},
		{
			name: "accepted_values",
			test: domain.ModelTest{
				TestType: "accepted_values", Column: "status",
				Config: domain.ModelTestConfig{Values: []string{"active", "inactive"}},
			},
			schema:  "analytics",
			model:   "orders",
			wantSQL: `SELECT * FROM "analytics"."orders" WHERE "status" NOT IN ('active', 'inactive') LIMIT 1`,
		},
		{
			name: "relationships",
			test: domain.ModelTest{
				TestType: "relationships", Column: "customer_id",
				Config: domain.ModelTestConfig{ToModel: "customers", ToColumn: "id"},
			},
			schema:  "analytics",
			model:   "orders",
			wantSQL: `SELECT a."customer_id" FROM "analytics"."orders" a LEFT JOIN "analytics"."customers" b ON a."customer_id" = b."id" WHERE b."id" IS NULL LIMIT 1`,
		},
		{
			name: "custom_sql",
			test: domain.ModelTest{
				TestType: "custom_sql",
				Config:   domain.ModelTestConfig{SQL: "SELECT 1 WHERE 1=0"},
			},
			schema:  "analytics",
			model:   "orders",
			wantSQL: "SELECT 1 WHERE 1=0",
		},
		{
			name:    "unknown type",
			test:    domain.ModelTest{TestType: "invalid"},
			schema:  "analytics",
			model:   "orders",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, err := generateTestSQL(tt.test, tt.schema, tt.model)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, sql)
		})
	}
}

func TestExecuteNotebookCellTests_SeverityGate(t *testing.T) {
	t.Run("error severity fails on rows", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		svc.notebookLinks = notebookLinkRepoStub{getByModelIDFn: func(_ context.Context, _ string) (*domain.NotebookModelLink, error) {
			return &domain.NotebookModelLink{NotebookID: "nb-1"}, nil
		}}
		svc.notebooks = notebookProviderStub{listCellsFn: func(_ context.Context, _ string) ([]domain.Cell, error) {
			return []domain.Cell{{
				ID:       "cell-test-1",
				CellType: domain.CellTypeSQL,
				Role:     domain.CellRoleTest,
				Content:  "SELECT 1",
				Test:     &domain.NotebookCellTestConfig{Severity: domain.NotebookTestSeverityError},
			}}, nil
		}}

		failed, err := svc.executeNotebookCellTests(context.Background(), conn, &domain.Model{ID: "m-1", ProjectName: "p", Name: "n"}, "step-1", "admin")
		require.NoError(t, err)
		assert.True(t, failed)
	})

	t.Run("warn severity does not fail on rows", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		svc.notebookLinks = notebookLinkRepoStub{getByModelIDFn: func(_ context.Context, _ string) (*domain.NotebookModelLink, error) {
			return &domain.NotebookModelLink{NotebookID: "nb-1"}, nil
		}}
		svc.notebooks = notebookProviderStub{listCellsFn: func(_ context.Context, _ string) ([]domain.Cell, error) {
			return []domain.Cell{{
				ID:       "cell-test-2",
				CellType: domain.CellTypeSQL,
				Role:     domain.CellRoleTest,
				Content:  "SELECT 1",
				Test:     &domain.NotebookCellTestConfig{Severity: domain.NotebookTestSeverityWarn},
			}}, nil
		}}

		failed, err := svc.executeNotebookCellTests(context.Background(), conn, &domain.Model{ID: "m-2", ProjectName: "p", Name: "n"}, "step-1", "admin")
		require.NoError(t, err)
		assert.False(t, failed)
	})
}
