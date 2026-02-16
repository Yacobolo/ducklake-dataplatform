package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
)

// === Test helpers (prefixed with "queryTest" to avoid collisions) ===

func queryTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: true,
	})
}

func queryTestStrPtr(s string) *string { return &s }

// === Mocks ===

type mockManifestService struct {
	getManifestFn func(ctx context.Context, principalName, catalogName, schemaName, tableName string) (*query.ManifestResult, error)
}

func (m *mockManifestService) GetManifest(ctx context.Context, principalName, catalogName, schemaName, tableName string) (*query.ManifestResult, error) {
	if m.getManifestFn == nil {
		panic("mockManifestService.GetManifest called but not configured")
	}
	return m.getManifestFn(ctx, principalName, catalogName, schemaName, tableName)
}

type mockCatalogServiceForQuery struct {
	profileTableFn func(ctx context.Context, catalogName string, principal string, schemaName, tableName string) (*domain.TableStatistics, error)
}

func (m *mockCatalogServiceForQuery) GetCatalogInfo(_ context.Context, _ string) (*domain.CatalogInfo, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) ListSchemas(_ context.Context, _ string, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) CreateSchema(_ context.Context, _ string, _ string, _ domain.CreateSchemaRequest) (*domain.SchemaDetail, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) GetSchema(_ context.Context, _ string, _ string) (*domain.SchemaDetail, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) UpdateSchema(_ context.Context, _ string, _ string, _ string, _ domain.UpdateSchemaRequest) (*domain.SchemaDetail, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) DeleteSchema(_ context.Context, _ string, _ string, _ string, _ bool) error {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) ListTables(_ context.Context, _ string, _ string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) CreateTable(_ context.Context, _ string, _ string, _ string, _ domain.CreateTableRequest) (*domain.TableDetail, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) GetTable(_ context.Context, _ string, _ string, _ string) (*domain.TableDetail, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) UpdateTable(_ context.Context, _ string, _ string, _ string, _ string, _ domain.UpdateTableRequest) (*domain.TableDetail, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) DeleteTable(_ context.Context, _ string, _ string, _ string, _ string) error {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) ListColumns(_ context.Context, _ string, _ string, _ string, _ domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) UpdateColumn(_ context.Context, _ string, _ string, _ string, _ string, _ string, _ domain.UpdateColumnRequest) (*domain.ColumnDetail, error) {
	panic("not implemented")
}
func (m *mockCatalogServiceForQuery) ProfileTable(ctx context.Context, catalogName string, principal string, schemaName, tableName string) (*domain.TableStatistics, error) {
	if m.profileTableFn == nil {
		panic("mockCatalogServiceForQuery.ProfileTable called but not configured")
	}
	return m.profileTableFn(ctx, catalogName, principal, schemaName, tableName)
}
func (m *mockCatalogServiceForQuery) GetMetastoreSummary(_ context.Context, _ string) (*domain.MetastoreSummary, error) {
	panic("not implemented")
}

// === CreateManifest Tests ===

func TestHandler_CreateManifest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     CreateManifestJSONRequestBody
		svcFn    func(ctx context.Context, principalName, catalogName, schemaName, tableName string) (*query.ManifestResult, error)
		assertFn func(t *testing.T, resp CreateManifestResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			body: CreateManifestJSONRequestBody{Table: "users", Schema: queryTestStrPtr("main")},
			svcFn: func(_ context.Context, _, _, _, _ string) (*query.ManifestResult, error) {
				return &query.ManifestResult{
					Table:       "users",
					Schema:      "main",
					Columns:     []query.ManifestColumn{{Name: "id", Type: "INTEGER"}},
					Files:       []string{"s3://bucket/data/file.parquet"},
					RowFilters:  []string{},
					ColumnMasks: map[string]string{},
					ExpiresAt:   time.Now().Add(time.Hour),
				}, nil
			},
			assertFn: func(t *testing.T, resp CreateManifestResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(CreateManifest200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Table)
				assert.Equal(t, "users", *ok200.Body.Table)
				require.NotNil(t, ok200.Body.Schema)
				assert.Equal(t, "main", *ok200.Body.Schema)
				require.NotNil(t, ok200.Body.Columns)
				require.Len(t, *ok200.Body.Columns, 1)
				assert.Equal(t, "id", *(*ok200.Body.Columns)[0].Name)
				require.NotNil(t, ok200.Body.Files)
				require.Len(t, *ok200.Body.Files, 1)
				assert.Equal(t, "s3://bucket/data/file.parquet", (*ok200.Body.Files)[0])
			},
		},
		{
			name: "not found returns 404",
			body: CreateManifestJSONRequestBody{Table: "nonexistent", Schema: queryTestStrPtr("main")},
			svcFn: func(_ context.Context, _, _, _, _ string) (*query.ManifestResult, error) {
				return nil, domain.ErrNotFound("table not found")
			},
			assertFn: func(t *testing.T, resp CreateManifestResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(CreateManifest404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			body: CreateManifestJSONRequestBody{Table: "secret", Schema: queryTestStrPtr("main")},
			svcFn: func(_ context.Context, _, _, _, _ string) (*query.ManifestResult, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CreateManifestResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateManifest403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "validation error returns 400",
			body: CreateManifestJSONRequestBody{Table: "", Schema: queryTestStrPtr("main")},
			svcFn: func(_ context.Context, _, _, _, _ string) (*query.ManifestResult, error) {
				return nil, domain.ErrValidation("table name is required")
			},
			assertFn: func(t *testing.T, resp CreateManifestResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateManifest400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "internal error returns 500",
			body: CreateManifestJSONRequestBody{Table: "users", Schema: queryTestStrPtr("main")},
			svcFn: func(_ context.Context, _, _, _, _ string) (*query.ManifestResult, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreateManifestResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				serverErr, ok := resp.(CreateManifest500JSONResponse)
				require.True(t, ok, "expected 500 response, got %T", resp)
				assert.Equal(t, int32(500), serverErr.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockManifestService{getManifestFn: tt.svcFn}
			handler := &APIHandler{manifest: svc}
			body := tt.body
			resp, err := handler.CreateManifest(queryTestCtx(), CreateManifestRequestObject{Body: &body})
			tt.assertFn(t, resp, err)
		})
	}
}

// === ProfileTable Tests ===

func TestHandler_ProfileTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, catalogName string, principal string, schemaName, tableName string) (*domain.TableStatistics, error)
		assertFn func(t *testing.T, resp ProfileTableResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _, _, _, _ string) (*domain.TableStatistics, error) {
				rc := int64(42)
				return &domain.TableStatistics{RowCount: &rc}, nil
			},
			assertFn: func(t *testing.T, resp ProfileTableResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ProfileTable200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.RowCount)
				assert.Equal(t, int64(42), *ok200.Body.RowCount)
			},
		},
		{
			name: "not found returns 404",
			svcFn: func(_ context.Context, _, _, _, _ string) (*domain.TableStatistics, error) {
				return nil, domain.ErrNotFound("table not found")
			},
			assertFn: func(t *testing.T, resp ProfileTableResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(ProfileTable404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _, _, _, _ string) (*domain.TableStatistics, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp ProfileTableResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(ProfileTable403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogServiceForQuery{profileTableFn: tt.svcFn}
			handler := &APIHandler{catalog: svc}
			resp, err := handler.ProfileTable(queryTestCtx(), ProfileTableRequestObject{
				CatalogName: "default",
				SchemaName:  "main",
				TableName:   "users",
			})
			tt.assertFn(t, resp, err)
		})
	}
}
