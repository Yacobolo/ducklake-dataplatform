package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Mocks ===

type mockCatalogRegistrationService struct {
	registerFn   func(ctx context.Context, req domain.CreateCatalogRequest) (*domain.CatalogRegistration, error)
	listFn       func(ctx context.Context, page domain.PageRequest) ([]domain.CatalogRegistration, int64, error)
	getFn        func(ctx context.Context, name string) (*domain.CatalogRegistration, error)
	updateFn     func(ctx context.Context, name string, req domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error)
	deleteFn     func(ctx context.Context, name string) error
	setDefaultFn func(ctx context.Context, name string) (*domain.CatalogRegistration, error)
}

func (m *mockCatalogRegistrationService) Register(ctx context.Context, req domain.CreateCatalogRequest) (*domain.CatalogRegistration, error) {
	if m.registerFn == nil {
		panic("mockCatalogRegistrationService.Register called but not configured")
	}
	return m.registerFn(ctx, req)
}

func (m *mockCatalogRegistrationService) List(ctx context.Context, page domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
	if m.listFn == nil {
		panic("mockCatalogRegistrationService.List called but not configured")
	}
	return m.listFn(ctx, page)
}

func (m *mockCatalogRegistrationService) Get(ctx context.Context, name string) (*domain.CatalogRegistration, error) {
	if m.getFn == nil {
		panic("mockCatalogRegistrationService.Get called but not configured")
	}
	return m.getFn(ctx, name)
}

func (m *mockCatalogRegistrationService) Update(ctx context.Context, name string, req domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
	if m.updateFn == nil {
		panic("mockCatalogRegistrationService.Update called but not configured")
	}
	return m.updateFn(ctx, name, req)
}

func (m *mockCatalogRegistrationService) Delete(ctx context.Context, name string) error {
	if m.deleteFn == nil {
		panic("mockCatalogRegistrationService.Delete called but not configured")
	}
	return m.deleteFn(ctx, name)
}

func (m *mockCatalogRegistrationService) SetDefault(ctx context.Context, name string) (*domain.CatalogRegistration, error) {
	if m.setDefaultFn == nil {
		panic("mockCatalogRegistrationService.SetDefault called but not configured")
	}
	return m.setDefaultFn(ctx, name)
}

// === Helpers ===

func catTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: true,
	})
}

var catFixedTime = time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

func catStrPtr(s string) *string { return &s }

func catSampleRegistration() domain.CatalogRegistration {
	return domain.CatalogRegistration{
		ID:            "c-1",
		Name:          "cat",
		MetastoreType: "ducklake",
		DSN:           "sqlite:test.db",
		DataPath:      "/data",
		Status:        "ACTIVE",
		IsDefault:     false,
		CreatedAt:     catFixedTime,
		UpdatedAt:     catFixedTime,
	}
}

// === Tests ===

func TestHandler_RegisterCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, req domain.CreateCatalogRequest) (*domain.CatalogRegistration, error)
		assertFn func(t *testing.T, resp RegisterCatalogResponseObject, err error)
	}{
		{
			name: "happy path returns 201",
			svcFn: func(_ context.Context, _ domain.CreateCatalogRequest) (*domain.CatalogRegistration, error) {
				r := catSampleRegistration()
				return &r, nil
			},
			assertFn: func(t *testing.T, resp RegisterCatalogResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(RegisterCatalog201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "c-1", *created.Body.Id)
				assert.Equal(t, "cat", created.Body.Name)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ domain.CreateCatalogRequest) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp RegisterCatalogResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(RegisterCatalog403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "validation error returns 400",
			svcFn: func(_ context.Context, _ domain.CreateCatalogRequest) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrValidation("name is required")
			},
			assertFn: func(t *testing.T, resp RegisterCatalogResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(RegisterCatalog400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "conflict returns 409",
			svcFn: func(_ context.Context, _ domain.CreateCatalogRequest) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrConflict("catalog already exists")
			},
			assertFn: func(t *testing.T, resp RegisterCatalogResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(RegisterCatalog409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogRegistrationService{registerFn: tt.svcFn}
			handler := &APIHandler{catalogRegistration: svc}
			body := RegisterCatalogJSONRequestBody{
				Name:          "cat",
				MetastoreType: "ducklake",
				Dsn:           "sqlite:test.db",
				DataPath:      "/data",
			}
			resp, err := handler.RegisterCatalog(catTestCtx(), RegisterCatalogRequestObject{Body: &body})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListCatalogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, page domain.PageRequest) ([]domain.CatalogRegistration, int64, error)
		assertFn func(t *testing.T, resp ListCatalogsResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
				return []domain.CatalogRegistration{catSampleRegistration()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListCatalogsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListCatalogs200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "cat", (*ok200.Body.Data)[0].Name)
			},
		},
		{
			name: "service error propagates",
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListCatalogsResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogRegistrationService{listFn: tt.svcFn}
			handler := &APIHandler{catalogRegistration: svc}
			resp, err := handler.ListCatalogs(catTestCtx(), ListCatalogsRequestObject{Params: ListCatalogsParams{}})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetCatalogRegistration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, name string) (*domain.CatalogRegistration, error)
		assertFn func(t *testing.T, resp GetCatalogRegistrationResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
				r := catSampleRegistration()
				return &r, nil
			},
			assertFn: func(t *testing.T, resp GetCatalogRegistrationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetCatalogRegistration200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "c-1", *ok200.Body.Id)
				assert.Equal(t, "cat", ok200.Body.Name)
			},
		},
		{
			name: "not found returns 404",
			svcFn: func(_ context.Context, name string) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrNotFound("catalog %s not found", name)
			},
			assertFn: func(t *testing.T, resp GetCatalogRegistrationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetCatalogRegistration404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogRegistrationService{getFn: tt.svcFn}
			handler := &APIHandler{catalogRegistration: svc}
			resp, err := handler.GetCatalogRegistration(catTestCtx(), GetCatalogRegistrationRequestObject{CatalogName: "cat"})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_UpdateCatalogRegistration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, name string, req domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error)
		assertFn func(t *testing.T, resp UpdateCatalogRegistrationResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string, _ domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
				r := catSampleRegistration()
				r.Comment = "updated"
				return &r, nil
			},
			assertFn: func(t *testing.T, resp UpdateCatalogRegistrationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(UpdateCatalogRegistration200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "c-1", *ok200.Body.Id)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string, _ domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp UpdateCatalogRegistrationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(UpdateCatalogRegistration403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "not found returns 404",
			svcFn: func(_ context.Context, name string, _ domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrNotFound("catalog %s not found", name)
			},
			assertFn: func(t *testing.T, resp UpdateCatalogRegistrationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(UpdateCatalogRegistration404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogRegistrationService{updateFn: tt.svcFn}
			handler := &APIHandler{catalogRegistration: svc}
			body := UpdateCatalogRegistrationJSONRequestBody{
				Comment: catStrPtr("updated"),
			}
			resp, err := handler.UpdateCatalogRegistration(catTestCtx(), UpdateCatalogRegistrationRequestObject{
				CatalogName: "cat",
				Body:        &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteCatalogRegistration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, name string) error
		assertFn func(t *testing.T, resp DeleteCatalogRegistrationResponseObject, err error)
	}{
		{
			name: "happy path returns 204",
			svcFn: func(_ context.Context, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteCatalogRegistrationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteCatalogRegistration204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string) error {
				return domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp DeleteCatalogRegistrationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteCatalogRegistration403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "not found returns 404",
			svcFn: func(_ context.Context, name string) error {
				return domain.ErrNotFound("catalog %s not found", name)
			},
			assertFn: func(t *testing.T, resp DeleteCatalogRegistrationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteCatalogRegistration404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogRegistrationService{deleteFn: tt.svcFn}
			handler := &APIHandler{catalogRegistration: svc}
			resp, err := handler.DeleteCatalogRegistration(catTestCtx(), DeleteCatalogRegistrationRequestObject{CatalogName: "cat"})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_SetDefaultCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, name string) (*domain.CatalogRegistration, error)
		assertFn func(t *testing.T, resp SetDefaultCatalogResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
				r := catSampleRegistration()
				r.IsDefault = true
				return &r, nil
			},
			assertFn: func(t *testing.T, resp SetDefaultCatalogResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(SetDefaultCatalog200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "c-1", *ok200.Body.Id)
				assert.True(t, *ok200.Body.IsDefault)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp SetDefaultCatalogResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(SetDefaultCatalog403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "not found returns 404",
			svcFn: func(_ context.Context, name string) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrNotFound("catalog %s not found", name)
			},
			assertFn: func(t *testing.T, resp SetDefaultCatalogResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(SetDefaultCatalog404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogRegistrationService{setDefaultFn: tt.svcFn}
			handler := &APIHandler{catalogRegistration: svc}
			resp, err := handler.SetDefaultCatalog(catTestCtx(), SetDefaultCatalogRequestObject{CatalogName: "cat"})
			tt.assertFn(t, resp, err)
		})
	}
}
