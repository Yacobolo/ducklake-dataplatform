package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type mockResourceAccessService struct {
	listRecentFn func(ctx context.Context, principal domain.ContextPrincipal, limit int) ([]domain.ResourceAccessEvent, error)
}

func (m *mockResourceAccessService) ListRecent(ctx context.Context, principal domain.ContextPrincipal, limit int) ([]domain.ResourceAccessEvent, error) {
	if m.listRecentFn == nil {
		panic("mockResourceAccessService.ListRecent called but not configured")
	}
	return m.listRecentFn(ctx, principal, limit)
}

type mockSavedResourceService struct {
	saveFn      func(ctx context.Context, principal domain.ContextPrincipal, resource domain.ResourceRef) error
	unsaveFn    func(ctx context.Context, principal domain.ContextPrincipal, resourceType string, resourceKey string) error
	listSavedFn func(ctx context.Context, principal domain.ContextPrincipal, limit int) ([]domain.SavedResource, error)
}

func (m *mockSavedResourceService) Save(ctx context.Context, principal domain.ContextPrincipal, resource domain.ResourceRef) error {
	if m.saveFn == nil {
		panic("mockSavedResourceService.Save called but not configured")
	}
	return m.saveFn(ctx, principal, resource)
}

func (m *mockSavedResourceService) Unsave(ctx context.Context, principal domain.ContextPrincipal, resourceType string, resourceKey string) error {
	if m.unsaveFn == nil {
		panic("mockSavedResourceService.Unsave called but not configured")
	}
	return m.unsaveFn(ctx, principal, resourceType, resourceKey)
}

func (m *mockSavedResourceService) ListSaved(ctx context.Context, principal domain.ContextPrincipal, limit int) ([]domain.SavedResource, error) {
	if m.listSavedFn == nil {
		panic("mockSavedResourceService.ListSaved called but not configured")
	}
	return m.listSavedFn(ctx, principal, limit)
}

func resourceTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		ID:      "11111111-1111-1111-1111-111111111111",
		Name:    "alice",
		Type:    "user",
		IsAdmin: false,
	})
}

func resourceInt32Ptr(v int32) *int32 { return &v }

func TestHandler_ListRecentResources(t *testing.T) {
	t.Parallel()

	accessedAt := time.Date(2026, 4, 1, 8, 30, 0, 0, time.UTC)
	handler := &APIHandler{
		resourceAccess: &mockResourceAccessService{
			listRecentFn: func(_ context.Context, principal domain.ContextPrincipal, limit int) ([]domain.ResourceAccessEvent, error) {
				assert.Equal(t, "11111111-1111-1111-1111-111111111111", principal.ID)
				assert.Equal(t, 5, limit)
				return []domain.ResourceAccessEvent{{
					ResourceRef: domain.ResourceRef{
						ResourceType: "notebook",
						ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7b",
						DisplayName:  "Quick Start Guide",
						ResourcePath: "Home",
						Href:         "/ui/notebooks/019d43e3-9377-79f6-a368-01b6ae805b7b",
						Section:      "Build",
					},
					AccessedAt: accessedAt,
				}}, nil
			},
		},
	}

	resp, err := handler.ListRecentResources(resourceTestCtx(), GenListRecentResourcesRequest{
		Params: GenListRecentResourcesParams{MaxResults: resourceInt32Ptr(5)},
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenListRecentResources200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.Len(t, okResp.Body.Data, 1)
	assert.Equal(t, "notebook", okResp.Body.Data[0].ResourceType)
	assert.Equal(t, "019d43e3-9377-79f6-a368-01b6ae805b7b", okResp.Body.Data[0].ResourceKey)
	assert.Equal(t, "Quick Start Guide", okResp.Body.Data[0].DisplayName)
	require.NotNil(t, okResp.Body.Data[0].ResourcePath)
	assert.Equal(t, "Home", *okResp.Body.Data[0].ResourcePath)
	require.NotNil(t, okResp.Body.Data[0].Href)
	assert.Equal(t, "/ui/notebooks/019d43e3-9377-79f6-a368-01b6ae805b7b", *okResp.Body.Data[0].Href)
	require.NotNil(t, okResp.Body.Data[0].AccessedAt)
	assert.Equal(t, accessedAt.Format(time.RFC3339), *okResp.Body.Data[0].AccessedAt)
}

func TestHandler_ListSavedResources(t *testing.T) {
	t.Parallel()

	savedAt := time.Date(2026, 4, 1, 7, 0, 0, 0, time.UTC)
	lastAccessedAt := time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)
	handler := &APIHandler{
		savedResources: &mockSavedResourceService{
			listSavedFn: func(_ context.Context, principal domain.ContextPrincipal, limit int) ([]domain.SavedResource, error) {
				assert.Equal(t, "11111111-1111-1111-1111-111111111111", principal.ID)
				assert.Equal(t, 4, limit)
				return []domain.SavedResource{{
					ResourceRef: domain.ResourceRef{
						ResourceType: "dashboard",
						ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805c99",
						DisplayName:  "Revenue Overview",
						ResourcePath: "Shared/Executive",
						Href:         "/ui/dashboards/019d43e3-9377-79f6-a368-01b6ae805c99",
						Section:      "Discover",
					},
					SavedAt:        savedAt,
					LastAccessedAt: &lastAccessedAt,
				}}, nil
			},
		},
	}

	resp, err := handler.ListSavedResources(resourceTestCtx(), GenListSavedResourcesRequest{
		Params: GenListSavedResourcesParams{MaxResults: resourceInt32Ptr(4)},
	})
	require.NoError(t, err)

	okResp, ok := resp.(GenListSavedResources200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.Len(t, okResp.Body.Data, 1)
	assert.Equal(t, "dashboard", okResp.Body.Data[0].ResourceType)
	assert.Equal(t, "Revenue Overview", okResp.Body.Data[0].DisplayName)
	require.NotNil(t, okResp.Body.Data[0].SavedAt)
	assert.Equal(t, savedAt.Format(time.RFC3339), *okResp.Body.Data[0].SavedAt)
	require.NotNil(t, okResp.Body.Data[0].LastAccessedAt)
	assert.Equal(t, lastAccessedAt.Format(time.RFC3339), *okResp.Body.Data[0].LastAccessedAt)
}

func TestHandler_CreateSavedResource(t *testing.T) {
	t.Parallel()

	handler := &APIHandler{
		savedResources: &mockSavedResourceService{
			saveFn: func(_ context.Context, principal domain.ContextPrincipal, resource domain.ResourceRef) error {
				assert.Equal(t, "11111111-1111-1111-1111-111111111111", principal.ID)
				assert.Equal(t, "dashboard", resource.ResourceType)
				assert.Equal(t, "019d43e3-9377-79f6-a368-01b6ae805c99", resource.ResourceKey)
				assert.Equal(t, "Revenue Overview", resource.DisplayName)
				assert.Equal(t, "Shared/Executive", resource.ResourcePath)
				assert.Equal(t, "/ui/dashboards/019d43e3-9377-79f6-a368-01b6ae805c99", resource.Href)
				return nil
			},
		},
	}

	displayName := "Revenue Overview"
	resourcePath := "Shared/Executive"
	section := "Discover"
	resp, err := handler.CreateSavedResource(resourceTestCtx(), GenCreateSavedResourceRequest{
		Body: &GenCreateSavedResourceJSONBody{
			ResourceType: "dashboard",
			ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805c99",
			DisplayName:  &displayName,
			ResourcePath: &resourcePath,
			Section:      &section,
		},
	})
	require.NoError(t, err)

	created, ok := resp.(GenCreateSavedResource201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)
	assert.Equal(t, "dashboard", created.Body.ResourceType)
	assert.Equal(t, "Revenue Overview", created.Body.DisplayName)
	require.NotNil(t, created.Body.Href)
	assert.Equal(t, "/ui/dashboards/019d43e3-9377-79f6-a368-01b6ae805c99", *created.Body.Href)
	require.NotNil(t, created.Body.SavedAt)
}

func TestHandler_DeleteSavedResource(t *testing.T) {
	t.Parallel()

	handler := &APIHandler{
		savedResources: &mockSavedResourceService{
			unsaveFn: func(_ context.Context, principal domain.ContextPrincipal, resourceType string, resourceKey string) error {
				assert.Equal(t, "11111111-1111-1111-1111-111111111111", principal.ID)
				assert.Equal(t, "notebook", resourceType)
				assert.Equal(t, "019d43e3-9377-79f6-a368-01b6ae805b7b", resourceKey)
				return nil
			},
		},
	}

	resp, err := handler.DeleteSavedResource(resourceTestCtx(), GenDeleteSavedResourceRequest{
		ResourceType: "notebook",
		ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7b",
	})
	require.NoError(t, err)

	_, ok := resp.(GenDeleteSavedResource204Response)
	require.True(t, ok, "expected 204 response, got %T", resp)
}
