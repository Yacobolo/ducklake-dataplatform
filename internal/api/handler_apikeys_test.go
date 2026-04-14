package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type mockAPIKeyService struct {
	createFn func(ctx context.Context, req domain.CreateAPIKeyRequest) (string, *domain.APIKey, error)
	deleteFn func(ctx context.Context, id string) error
}

func (m *mockAPIKeyService) Create(ctx context.Context, req domain.CreateAPIKeyRequest) (string, *domain.APIKey, error) {
	if m.createFn == nil {
		panic("mockAPIKeyService.Create called but not configured")
	}
	return m.createFn(ctx, req)
}

func (m *mockAPIKeyService) List(context.Context, *string, domain.PageRequest) ([]domain.APIKey, int64, error) {
	panic("not implemented")
}

func (m *mockAPIKeyService) Delete(ctx context.Context, id string) error {
	if m.deleteFn == nil {
		panic("mockAPIKeyService.Delete called but not configured")
	}
	return m.deleteFn(ctx, id)
}

func (m *mockAPIKeyService) CleanupExpired(context.Context) (int64, error) {
	panic("not implemented")
}

func TestHandler_CreateAPIKey_UnknownPrincipalReturns400(t *testing.T) {
	t.Parallel()

	handler := &APIHandler{apiKeys: &mockAPIKeyService{
		createFn: func(_ context.Context, _ domain.CreateAPIKeyRequest) (string, *domain.APIKey, error) {
			return "", nil, domain.ErrNotFound("principal not found")
		},
	}}

	body := CreateAPIKeyJSONRequestBody{
		PrincipalId: "00000000-0000-0000-0000-000000000000",
		Name:        strPtr("example"),
	}
	resp, err := handler.CreateAPIKey(secTestCtx(), GenCreateAPIKeyRequest{Body: &body})
	require.NoError(t, err)

	badReq, ok := resp.(CreateAPIKey400JSONResponse)
	require.True(t, ok, "expected 400 response, got %T", resp)
	assert.Equal(t, int32(400), badReq.Body.Code)
}

func TestHandler_CreateAPIKey_CreatesResponse(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 3, 8, 10, 0, 0, 0, time.UTC)
	handler := &APIHandler{apiKeys: &mockAPIKeyService{
		createFn: func(_ context.Context, _ domain.CreateAPIKeyRequest) (string, *domain.APIKey, error) {
			return "raw-secret", &domain.APIKey{
				ID:        "key-1",
				Name:      "example",
				KeyPrefix: "raw-secr",
				CreatedAt: createdAt,
			}, nil
		},
	}}

	body := CreateAPIKeyJSONRequestBody{
		PrincipalId: "00000000-0000-0000-0000-000000000001",
		Name:        strPtr("example"),
	}
	resp, err := handler.CreateAPIKey(secTestCtx(), GenCreateAPIKeyRequest{Body: &body})
	require.NoError(t, err)

	created, ok := resp.(CreateAPIKey201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)
	assert.Equal(t, "raw-secret", created.Body.Key)
	assert.Equal(t, "key-1", created.Body.Id)
}

func TestHandler_DeleteAPIKey_AccessDeniedReturns403(t *testing.T) {
	t.Parallel()

	handler := &APIHandler{apiKeys: &mockAPIKeyService{
		deleteFn: func(_ context.Context, _ string) error {
			return domain.ErrAccessDenied("cannot delete another principal's key")
		},
	}}

	resp, err := handler.DeleteAPIKey(secTestCtx(), GenDeleteAPIKeyRequest{ApiKeyId: "key-1"})
	require.NoError(t, err)

	forbidden, ok := resp.(DeleteAPIKey403JSONResponse)
	require.True(t, ok, "expected 403 response, got %T", resp)
	assert.Equal(t, int32(403), forbidden.Body.Code)
}
