package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

func ctxWithPrincipal(name string) context.Context {
	return middleware.WithPrincipal(context.Background(), name)
}

func newTestViewService(viewRepo *mockViewRepo, catalog *mockCatalogRepo, auth *mockAuthService, audit *mockAuditRepo) *ViewService {
	return NewViewService(viewRepo, catalog, auth, audit)
}

// === CreateView ===

func TestViewService_CreateView(t *testing.T) {
	schema := &domain.SchemaDetail{
		SchemaID:    42,
		Name:        "main",
		CatalogName: "lake",
	}
	req := domain.CreateViewRequest{
		Name:           "v_test",
		ViewDefinition: "SELECT 1",
		Comment:        "test view",
	}

	t.Run("happy_path", func(t *testing.T) {
		viewRepo := &mockViewRepo{
			createFn: func(_ context.Context, v *domain.ViewDetail) (*domain.ViewDetail, error) {
				return &domain.ViewDetail{
					ID:             1,
					SchemaID:       v.SchemaID,
					Name:           v.Name,
					ViewDefinition: v.ViewDefinition,
					Comment:        v.Comment,
					Owner:          v.Owner,
					CreatedAt:      time.Now(),
				}, nil
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}

		svc := newTestViewService(viewRepo, catalog, auth, audit)
		result, err := svc.CreateView(ctxWithPrincipal("alice"), "main", req)

		require.NoError(t, err)
		assert.Equal(t, "v_test", result.Name)
		assert.Equal(t, "main", result.SchemaName)
		assert.Equal(t, "lake", result.CatalogName)
		assert.Equal(t, "alice", result.Owner)
	})

	t.Run("sets_owner_from_principal", func(t *testing.T) {
		var captured *domain.ViewDetail
		viewRepo := &mockViewRepo{
			createFn: func(_ context.Context, v *domain.ViewDetail) (*domain.ViewDetail, error) {
				captured = v
				return &domain.ViewDetail{ID: 1, Name: v.Name, Owner: v.Owner}, nil
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}

		svc := newTestViewService(viewRepo, catalog, auth, audit)
		_, err := svc.CreateView(ctxWithPrincipal("bob"), "main", req)

		require.NoError(t, err)
		require.NotNil(t, captured)
		assert.Equal(t, "bob", captured.Owner)
	})

	t.Run("access_denied", func(t *testing.T) {
		catalog := &mockCatalogRepo{}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return false, nil
			},
		}
		audit := &mockAuditRepo{}

		svc := newTestViewService(&mockViewRepo{}, catalog, auth, audit)
		_, err := svc.CreateView(ctxWithPrincipal("analyst"), "main", req)

		require.Error(t, err)
		var accessErr *domain.AccessDeniedError
		assert.True(t, errors.As(err, &accessErr))
	})

	t.Run("auth_check_error", func(t *testing.T) {
		catalog := &mockCatalogRepo{}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return false, errTest
			},
		}
		audit := &mockAuditRepo{}

		svc := newTestViewService(&mockViewRepo{}, catalog, auth, audit)
		_, err := svc.CreateView(ctxWithPrincipal("alice"), "main", req)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "check privilege:")
	})

	t.Run("schema_not_found", func(t *testing.T) {
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return nil, domain.ErrNotFound("schema %q not found", "bad")
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}

		svc := newTestViewService(&mockViewRepo{}, catalog, auth, audit)
		_, err := svc.CreateView(ctxWithPrincipal("alice"), "bad", req)

		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.True(t, errors.As(err, &notFound))
	})

	t.Run("repo_create_error", func(t *testing.T) {
		viewRepo := &mockViewRepo{
			createFn: func(_ context.Context, _ *domain.ViewDetail) (*domain.ViewDetail, error) {
				return nil, errTest
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}

		svc := newTestViewService(viewRepo, catalog, auth, audit)
		_, err := svc.CreateView(ctxWithPrincipal("alice"), "main", req)

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})

	t.Run("audit_logged", func(t *testing.T) {
		viewRepo := &mockViewRepo{
			createFn: func(_ context.Context, v *domain.ViewDetail) (*domain.ViewDetail, error) {
				return &domain.ViewDetail{ID: 1, Name: v.Name, Owner: v.Owner}, nil
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}

		svc := newTestViewService(viewRepo, catalog, auth, audit)
		_, err := svc.CreateView(ctxWithPrincipal("alice"), "main", req)

		require.NoError(t, err)
		require.NotNil(t, audit.lastEntry())
		assert.Equal(t, "CREATE_VIEW", audit.lastEntry().Action)
		assert.Equal(t, "alice", audit.lastEntry().PrincipalName)
	})
}

// === GetView ===

func TestViewService_GetView(t *testing.T) {
	schema := &domain.SchemaDetail{
		SchemaID:    42,
		Name:        "main",
		CatalogName: "lake",
	}

	t.Run("happy_path", func(t *testing.T) {
		viewRepo := &mockViewRepo{
			getByNameFn: func(_ context.Context, _ int64, _ string) (*domain.ViewDetail, error) {
				return &domain.ViewDetail{ID: 1, Name: "v_test"}, nil
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}

		svc := NewViewService(viewRepo, catalog, &mockAuthService{}, &mockAuditRepo{})
		result, err := svc.GetView(context.Background(), "main", "v_test")

		require.NoError(t, err)
		assert.Equal(t, "v_test", result.Name)
		assert.Equal(t, "main", result.SchemaName)
		assert.Equal(t, "lake", result.CatalogName)
	})

	t.Run("schema_not_found", func(t *testing.T) {
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return nil, domain.ErrNotFound("schema not found")
			},
		}

		svc := NewViewService(&mockViewRepo{}, catalog, &mockAuthService{}, &mockAuditRepo{})
		_, err := svc.GetView(context.Background(), "bad", "v_test")

		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.True(t, errors.As(err, &notFound))
	})

	t.Run("view_not_found", func(t *testing.T) {
		viewRepo := &mockViewRepo{
			getByNameFn: func(_ context.Context, _ int64, _ string) (*domain.ViewDetail, error) {
				return nil, domain.ErrNotFound("view not found")
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}

		svc := NewViewService(viewRepo, catalog, &mockAuthService{}, &mockAuditRepo{})
		_, err := svc.GetView(context.Background(), "main", "nonexistent")

		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.True(t, errors.As(err, &notFound))
	})
}

// === ListViews ===

func TestViewService_ListViews(t *testing.T) {
	schema := &domain.SchemaDetail{
		SchemaID:    42,
		Name:        "main",
		CatalogName: "lake",
	}
	page := domain.PageRequest{MaxResults: 100}

	t.Run("happy_path", func(t *testing.T) {
		viewRepo := &mockViewRepo{
			listFn: func(_ context.Context, _ int64, _ domain.PageRequest) ([]domain.ViewDetail, int64, error) {
				return []domain.ViewDetail{
					{ID: 1, Name: "v1"},
					{ID: 2, Name: "v2"},
				}, 2, nil
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}

		svc := NewViewService(viewRepo, catalog, &mockAuthService{}, &mockAuditRepo{})
		views, total, err := svc.ListViews(context.Background(), "main", page)

		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		require.Len(t, views, 2)
		for _, v := range views {
			assert.Equal(t, "main", v.SchemaName)
			assert.Equal(t, "lake", v.CatalogName)
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		viewRepo := &mockViewRepo{
			listFn: func(_ context.Context, _ int64, _ domain.PageRequest) ([]domain.ViewDetail, int64, error) {
				return []domain.ViewDetail{}, 0, nil
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}

		svc := NewViewService(viewRepo, catalog, &mockAuthService{}, &mockAuditRepo{})
		views, total, err := svc.ListViews(context.Background(), "main", page)

		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Empty(t, views)
	})

	t.Run("schema_not_found", func(t *testing.T) {
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return nil, domain.ErrNotFound("schema not found")
			},
		}

		svc := NewViewService(&mockViewRepo{}, catalog, &mockAuthService{}, &mockAuditRepo{})
		_, _, err := svc.ListViews(context.Background(), "bad", page)

		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.True(t, errors.As(err, &notFound))
	})
}

// === DeleteView ===

func TestViewService_DeleteView(t *testing.T) {
	schema := &domain.SchemaDetail{
		SchemaID:    42,
		Name:        "main",
		CatalogName: "lake",
	}

	t.Run("happy_path", func(t *testing.T) {
		viewRepo := &mockViewRepo{
			deleteFn: func(_ context.Context, _ int64, _ string) error {
				return nil
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}

		svc := newTestViewService(viewRepo, catalog, auth, audit)
		err := svc.DeleteView(ctxWithPrincipal("alice"), "main", "v_test")

		require.NoError(t, err)
		require.NotNil(t, audit.lastEntry())
		assert.Equal(t, "DROP_VIEW", audit.lastEntry().Action)
	})

	t.Run("access_denied", func(t *testing.T) {
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return false, nil
			},
		}

		svc := newTestViewService(&mockViewRepo{}, &mockCatalogRepo{}, auth, &mockAuditRepo{})
		err := svc.DeleteView(ctxWithPrincipal("analyst"), "main", "v_test")

		require.Error(t, err)
		var accessErr *domain.AccessDeniedError
		assert.True(t, errors.As(err, &accessErr))
	})

	t.Run("schema_not_found", func(t *testing.T) {
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return nil, domain.ErrNotFound("schema not found")
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}

		svc := newTestViewService(&mockViewRepo{}, catalog, auth, &mockAuditRepo{})
		err := svc.DeleteView(ctxWithPrincipal("alice"), "bad", "v_test")

		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.True(t, errors.As(err, &notFound))
	})

	t.Run("repo_delete_error", func(t *testing.T) {
		viewRepo := &mockViewRepo{
			deleteFn: func(_ context.Context, _ int64, _ string) error {
				return errTest
			},
		}
		catalog := &mockCatalogRepo{
			getSchemaFn: func(_ context.Context, _ string) (*domain.SchemaDetail, error) {
				return schema, nil
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}

		svc := newTestViewService(viewRepo, catalog, auth, &mockAuditRepo{})
		err := svc.DeleteView(ctxWithPrincipal("alice"), "main", "v_test")

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}
