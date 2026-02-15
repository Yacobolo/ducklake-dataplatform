package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Mock ===

type mockViewService struct {
	listViewsFn  func(ctx context.Context, catalogName string, schemaName string, page domain.PageRequest) ([]domain.ViewDetail, int64, error)
	createViewFn func(ctx context.Context, catalogName string, principal string, schemaName string, req domain.CreateViewRequest) (*domain.ViewDetail, error)
	getViewFn    func(ctx context.Context, catalogName string, schemaName, viewName string) (*domain.ViewDetail, error)
	updateViewFn func(ctx context.Context, catalogName string, principal string, schemaName, viewName string, req domain.UpdateViewRequest) (*domain.ViewDetail, error)
	deleteViewFn func(ctx context.Context, catalogName string, principal string, schemaName, viewName string) error
}

func (m *mockViewService) ListViews(ctx context.Context, catalogName string, schemaName string, page domain.PageRequest) ([]domain.ViewDetail, int64, error) {
	if m.listViewsFn == nil {
		panic("mockViewService.ListViews called but not configured")
	}
	return m.listViewsFn(ctx, catalogName, schemaName, page)
}

func (m *mockViewService) CreateView(ctx context.Context, catalogName string, principal string, schemaName string, req domain.CreateViewRequest) (*domain.ViewDetail, error) {
	if m.createViewFn == nil {
		panic("mockViewService.CreateView called but not configured")
	}
	return m.createViewFn(ctx, catalogName, principal, schemaName, req)
}

func (m *mockViewService) GetView(ctx context.Context, catalogName string, schemaName, viewName string) (*domain.ViewDetail, error) {
	if m.getViewFn == nil {
		panic("mockViewService.GetView called but not configured")
	}
	return m.getViewFn(ctx, catalogName, schemaName, viewName)
}

func (m *mockViewService) UpdateView(ctx context.Context, catalogName string, principal string, schemaName, viewName string, req domain.UpdateViewRequest) (*domain.ViewDetail, error) {
	if m.updateViewFn == nil {
		panic("mockViewService.UpdateView called but not configured")
	}
	return m.updateViewFn(ctx, catalogName, principal, schemaName, viewName, req)
}

func (m *mockViewService) DeleteView(ctx context.Context, catalogName string, principal string, schemaName, viewName string) error {
	if m.deleteViewFn == nil {
		panic("mockViewService.DeleteView called but not configured")
	}
	return m.deleteViewFn(ctx, catalogName, principal, schemaName, viewName)
}

// === Helpers ===

func viewTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: true,
	})
}

var viewFixedTime = time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

func viewStrPtr(s string) *string { return &s }

func sampleViewDetail() domain.ViewDetail {
	return domain.ViewDetail{
		ID:             "view-1",
		SchemaID:       "schema-1",
		SchemaName:     "test-schema",
		CatalogName:    "test-catalog",
		Name:           "my-view",
		ViewDefinition: "SELECT 1",
		Comment:        viewStrPtr("test view"),
		Properties:     map[string]string{},
		Owner:          "test-user",
		SourceTables:   []string{},
		CreatedAt:      viewFixedTime,
		UpdatedAt:      viewFixedTime,
	}
}

// === Tests ===

func TestHandler_ListViews(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, catalogName string, schemaName string, page domain.PageRequest) ([]domain.ViewDetail, int64, error)
		assertFn func(t *testing.T, resp ListViewsResponseObject, err error)
	}{
		{
			name: "happy path returns 200 with results",
			svcFn: func(_ context.Context, _ string, _ string, _ domain.PageRequest) ([]domain.ViewDetail, int64, error) {
				return []domain.ViewDetail{sampleViewDetail()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListViewsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListViews200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "my-view", *(*ok200.Body.Data)[0].Name)
			},
		},
		{
			name: "not found schema returns 404",
			svcFn: func(_ context.Context, _ string, schemaName string, _ domain.PageRequest) ([]domain.ViewDetail, int64, error) {
				return nil, 0, domain.ErrNotFound("schema %s not found", schemaName)
			},
			assertFn: func(t *testing.T, resp ListViewsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(ListViews404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "service error propagates",
			svcFn: func(_ context.Context, _ string, _ string, _ domain.PageRequest) ([]domain.ViewDetail, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListViewsResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockViewService{listViewsFn: tt.svcFn}
			handler := &APIHandler{views: svc}
			resp, err := handler.ListViews(viewTestCtx(), ListViewsRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				Params:      ListViewsParams{},
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     CreateViewJSONRequestBody
		svcFn    func(ctx context.Context, catalogName string, principal string, schemaName string, req domain.CreateViewRequest) (*domain.ViewDetail, error)
		assertFn func(t *testing.T, resp CreateViewResponseObject, err error)
	}{
		{
			name: "happy path returns 201",
			body: CreateViewJSONRequestBody{Name: "my-view", ViewDefinition: "SELECT 1"},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateViewRequest) (*domain.ViewDetail, error) {
				v := sampleViewDetail()
				return &v, nil
			},
			assertFn: func(t *testing.T, resp CreateViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreateView201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "my-view", *created.Body.Name)
				assert.Equal(t, "view-1", *created.Body.Id)
			},
		},
		{
			name: "validation error returns 400",
			body: CreateViewJSONRequestBody{Name: "", ViewDefinition: ""},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateViewRequest) (*domain.ViewDetail, error) {
				return nil, domain.ErrValidation("view name is required")
			},
			assertFn: func(t *testing.T, resp CreateViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateView400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "view name is required")
			},
		},
		{
			name: "access denied returns 403",
			body: CreateViewJSONRequestBody{Name: "v", ViewDefinition: "SELECT 1"},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateViewRequest) (*domain.ViewDetail, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CreateViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateView403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "conflict error returns 409",
			body: CreateViewJSONRequestBody{Name: "my-view", ViewDefinition: "SELECT 1"},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateViewRequest) (*domain.ViewDetail, error) {
				return nil, domain.ErrConflict("view my-view already exists")
			},
			assertFn: func(t *testing.T, resp CreateViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreateView409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
				assert.Contains(t, conflict.Body.Message, "already exists")
			},
		},
		{
			name: "unknown error falls through to 400",
			body: CreateViewJSONRequestBody{Name: "fail", ViewDefinition: "SELECT 1"},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateViewRequest) (*domain.ViewDetail, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreateViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateView400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockViewService{createViewFn: tt.svcFn}
			handler := &APIHandler{views: svc}
			body := tt.body
			resp, err := handler.CreateView(viewTestCtx(), CreateViewRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				Body:        &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		viewName string
		svcFn    func(ctx context.Context, catalogName string, schemaName, viewName string) (*domain.ViewDetail, error)
		assertFn func(t *testing.T, resp GetViewResponseObject, err error)
	}{
		{
			name:     "happy path returns 200",
			viewName: "my-view",
			svcFn: func(_ context.Context, _ string, _, _ string) (*domain.ViewDetail, error) {
				v := sampleViewDetail()
				return &v, nil
			},
			assertFn: func(t *testing.T, resp GetViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetView200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "my-view", *ok200.Body.Name)
			},
		},
		{
			name:     "not found returns 404",
			viewName: "nonexistent",
			svcFn: func(_ context.Context, _ string, _, viewName string) (*domain.ViewDetail, error) {
				return nil, domain.ErrNotFound("view %s not found", viewName)
			},
			assertFn: func(t *testing.T, resp GetViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetView404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
				assert.Contains(t, notFound.Body.Message, "nonexistent")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockViewService{getViewFn: tt.svcFn}
			handler := &APIHandler{views: svc}
			resp, err := handler.GetView(viewTestCtx(), GetViewRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				ViewName:    tt.viewName,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_UpdateView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		viewName string
		body     UpdateViewJSONRequestBody
		svcFn    func(ctx context.Context, catalogName string, principal string, schemaName, viewName string, req domain.UpdateViewRequest) (*domain.ViewDetail, error)
		assertFn func(t *testing.T, resp UpdateViewResponseObject, err error)
	}{
		{
			name:     "happy path returns 200",
			viewName: "my-view",
			body:     UpdateViewJSONRequestBody{Comment: viewStrPtr("updated comment")},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ domain.UpdateViewRequest) (*domain.ViewDetail, error) {
				v := sampleViewDetail()
				v.Comment = viewStrPtr("updated comment")
				return &v, nil
			},
			assertFn: func(t *testing.T, resp UpdateViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(UpdateView200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "my-view", *ok200.Body.Name)
			},
		},
		{
			name:     "access denied returns 403",
			viewName: "my-view",
			body:     UpdateViewJSONRequestBody{Comment: viewStrPtr("x")},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ domain.UpdateViewRequest) (*domain.ViewDetail, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp UpdateViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(UpdateView403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:     "not found returns 404",
			viewName: "nonexistent",
			body:     UpdateViewJSONRequestBody{Comment: viewStrPtr("x")},
			svcFn: func(_ context.Context, _ string, _ string, _, viewName string, _ domain.UpdateViewRequest) (*domain.ViewDetail, error) {
				return nil, domain.ErrNotFound("view %s not found", viewName)
			},
			assertFn: func(t *testing.T, resp UpdateViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(UpdateView404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockViewService{updateViewFn: tt.svcFn}
			handler := &APIHandler{views: svc}
			body := tt.body
			resp, err := handler.UpdateView(viewTestCtx(), UpdateViewRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				ViewName:    tt.viewName,
				Body:        &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		viewName string
		svcFn    func(ctx context.Context, catalogName string, principal string, schemaName, viewName string) error
		assertFn func(t *testing.T, resp DeleteViewResponseObject, err error)
	}{
		{
			name:     "happy path returns 204",
			viewName: "my-view",
			svcFn: func(_ context.Context, _ string, _ string, _, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteView204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:     "access denied returns 403",
			viewName: "my-view",
			svcFn: func(_ context.Context, _ string, _ string, _, _ string) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteView403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:     "not found returns 404",
			viewName: "nonexistent",
			svcFn: func(_ context.Context, _ string, _ string, _, viewName string) error {
				return domain.ErrNotFound("view %s not found", viewName)
			},
			assertFn: func(t *testing.T, resp DeleteViewResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteView404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockViewService{deleteViewFn: tt.svcFn}
			handler := &APIHandler{views: svc}
			resp, err := handler.DeleteView(viewTestCtx(), DeleteViewRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				ViewName:    tt.viewName,
			})
			tt.assertFn(t, resp, err)
		})
	}
}
