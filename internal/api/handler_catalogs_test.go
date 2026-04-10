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

func catStrPtr(s string) *string                         { return &s }
func catMetastoreTypePtr(v MetastoreType) *MetastoreType { return &v }

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
		assertFn func(t *testing.T, resp GenRegisterCatalogResponse, err error)
	}{
		{
			name: "happy path returns 201",
			svcFn: func(_ context.Context, _ domain.CreateCatalogRequest) (*domain.CatalogRegistration, error) {
				r := catSampleRegistration()
				return &r, nil
			},
			assertFn: func(t *testing.T, resp GenRegisterCatalogResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(GenRegisterCatalog201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "c-1", created.Body.Id)
				assert.Equal(t, "cat", created.Body.Name)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ domain.CreateCatalogRequest) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp GenRegisterCatalogResponse, err error) {
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
			assertFn: func(t *testing.T, resp GenRegisterCatalogResponse, err error) {
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
			assertFn: func(t *testing.T, resp GenRegisterCatalogResponse, err error) {
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
			body := GenRegisterCatalogJSONBody{
				Name:          "cat",
				MetastoreType: catMetastoreTypePtr(MetastoreType("ducklake")),
				Dsn:           catStrPtr("sqlite:test.db"),
				DataPath:      catStrPtr("/data"),
			}
			resp, err := handler.RegisterCatalog(catTestCtx(), GenRegisterCatalogRequest{Body: &body})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListCatalogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, page domain.PageRequest) ([]domain.CatalogRegistration, int64, error)
		assertFn func(t *testing.T, resp GenListCatalogsResponse, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
				return []domain.CatalogRegistration{catSampleRegistration()}, 1, nil
			},
			assertFn: func(t *testing.T, resp GenListCatalogsResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GenListCatalogs200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.Len(t, ok200.Body.Catalogs, 1)
				assert.Equal(t, "cat", ok200.Body.Catalogs[0].Name)
			},
		},
		{
			name: "service error returns 500",
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp GenListCatalogsResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				internal, ok := resp.(GenListCatalogs500JSONResponse)
				require.True(t, ok, "expected 500 response, got %T", resp)
				assert.Equal(t, int32(500), internal.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
				return nil, 0, domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp GenListCatalogsResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(GenListCatalogs403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogRegistrationService{listFn: tt.svcFn}
			handler := &APIHandler{catalogRegistration: svc}
			resp, err := handler.ListCatalogs(catTestCtx(), GenListCatalogsRequest{Params: GenListCatalogsParams{}})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, name string) (*domain.CatalogRegistration, error)
		assertFn func(t *testing.T, resp GenGetCatalogResponse, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
				r := catSampleRegistration()
				return &r, nil
			},
			assertFn: func(t *testing.T, resp GenGetCatalogResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GenGetCatalog200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "c-1", ok200.Body.Id)
				assert.Equal(t, "cat", ok200.Body.Name)
			},
		},
		{
			name: "not found returns 404",
			svcFn: func(_ context.Context, name string) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrNotFound("catalog %s not found", name)
			},
			assertFn: func(t *testing.T, resp GenGetCatalogResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GenGetCatalog404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp GenGetCatalogResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(GenGetCatalog403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogRegistrationService{getFn: tt.svcFn}
			handler := &APIHandler{catalogRegistration: svc}
			resp, err := handler.GetCatalog(catTestCtx(), GenGetCatalogRequest{CatalogName: "cat"})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_UpdateCatalogRegistration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, name string, req domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error)
		assertFn func(t *testing.T, resp GenUpdateCatalogRegistrationResponse, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string, _ domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
				r := catSampleRegistration()
				r.Comment = "updated"
				return &r, nil
			},
			assertFn: func(t *testing.T, resp GenUpdateCatalogRegistrationResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GenUpdateCatalogRegistration200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "c-1", ok200.Body.Id)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string, _ domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp GenUpdateCatalogRegistrationResponse, err error) {
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
			assertFn: func(t *testing.T, resp GenUpdateCatalogRegistrationResponse, err error) {
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
			body := GenUpdateCatalogRegistrationJSONBody{
				Comment: catStrPtr("updated"),
			}
			resp, err := handler.UpdateCatalogRegistration(catTestCtx(), GenUpdateCatalogRegistrationRequest{
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
		assertFn func(t *testing.T, resp GenDeleteCatalogRegistrationResponse, err error)
	}{
		{
			name: "happy path returns 204",
			svcFn: func(_ context.Context, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp GenDeleteCatalogRegistrationResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(GenDeleteCatalogRegistration204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string) error {
				return domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp GenDeleteCatalogRegistrationResponse, err error) {
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
			assertFn: func(t *testing.T, resp GenDeleteCatalogRegistrationResponse, err error) {
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
			resp, err := handler.DeleteCatalogRegistration(catTestCtx(), GenDeleteCatalogRegistrationRequest{CatalogName: "cat"})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_SetDefaultCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, name string) (*domain.CatalogRegistration, error)
		assertFn func(t *testing.T, resp GenSetDefaultCatalogResponse, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
				r := catSampleRegistration()
				r.IsDefault = true
				return &r, nil
			},
			assertFn: func(t *testing.T, resp GenSetDefaultCatalogResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(SetDefaultCatalog200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "c-1", ok200.Body.Id)
				assert.True(t, *ok200.Body.IsDefault)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrAccessDenied("admin required")
			},
			assertFn: func(t *testing.T, resp GenSetDefaultCatalogResponse, err error) {
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
			assertFn: func(t *testing.T, resp GenSetDefaultCatalogResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(SetDefaultCatalog404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "validation returns 400",
			svcFn: func(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
				return nil, domain.ErrValidation("catalog must be ACTIVE")
			},
			assertFn: func(t *testing.T, resp GenSetDefaultCatalogResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(SetDefaultCatalog400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockCatalogRegistrationService{setDefaultFn: tt.svcFn}
			handler := &APIHandler{catalogRegistration: svc}
			resp, err := handler.SetDefaultCatalog(catTestCtx(), GenSetDefaultCatalogRequest{CatalogName: "cat"})
			tt.assertFn(t, resp, err)
		})
	}
}
