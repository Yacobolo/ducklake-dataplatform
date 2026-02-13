package api

import (
	"context"
	"errors"
	"time"

	"duck-demo/internal/domain"
)

// apiKeyService defines the API key management operations used by the API handler.
type apiKeyService interface {
	Create(ctx context.Context, principalID int64, name string, expiresAt *time.Time) (string, *domain.APIKey, error)
	List(ctx context.Context, principalID int64, page domain.PageRequest) ([]domain.APIKey, int64, error)
	Delete(ctx context.Context, id int64) error
	CleanupExpired(ctx context.Context) (int64, error)
}

// === API Keys ===

// CreateAPIKey implements the endpoint for creating a new API key.
func (h *APIHandler) CreateAPIKey(ctx context.Context, req CreateAPIKeyRequestObject) (CreateAPIKeyResponseObject, error) {
	rawKey, key, err := h.apiKeys.Create(ctx, req.Body.PrincipalId, req.Body.Name, req.Body.ExpiresAt)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return CreateAPIKey400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateAPIKey403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateAPIKey201JSONResponse{
		Body: CreateAPIKeyResponse{
			Id:        &key.ID,
			Key:       &rawKey,
			Name:      &key.Name,
			KeyPrefix: &key.KeyPrefix,
			ExpiresAt: key.ExpiresAt,
			CreatedAt: &key.CreatedAt,
		},
		Headers: CreateAPIKey201ResponseHeaders{
			XRateLimitLimit:     defaultRateLimitLimit,
			XRateLimitRemaining: defaultRateLimitRemaining,
			XRateLimitReset:     defaultRateLimitReset,
		},
	}, nil
}

// ListAPIKeys implements the endpoint for listing API keys for a principal.
func (h *APIHandler) ListAPIKeys(ctx context.Context, req ListAPIKeysRequestObject) (ListAPIKeysResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	keys, total, err := h.apiKeys.List(ctx, req.Params.PrincipalId, page)
	if err != nil {
		return nil, err
	}
	data := make([]APIKeyInfo, len(keys))
	for i, k := range keys {
		data[i] = apiKeyToAPI(k)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAPIKeys200JSONResponse{
		Body:    PaginatedAPIKeys{Data: &data, NextPageToken: optStr(npt)},
		Headers: ListAPIKeys200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteAPIKey implements the endpoint for deleting an API key by ID.
func (h *APIHandler) DeleteAPIKey(ctx context.Context, req DeleteAPIKeyRequestObject) (DeleteAPIKeyResponseObject, error) {
	if err := h.apiKeys.Delete(ctx, req.ApiKeyId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteAPIKey404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteAPIKey204Response{}, nil
}

// CleanupExpiredAPIKeys implements the endpoint for removing expired API keys.
func (h *APIHandler) CleanupExpiredAPIKeys(ctx context.Context, _ CleanupExpiredAPIKeysRequestObject) (CleanupExpiredAPIKeysResponseObject, error) {
	count, err := h.apiKeys.CleanupExpired(ctx)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CleanupExpiredAPIKeys403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CleanupExpiredAPIKeys200JSONResponse{
		Body:    CleanupAPIKeysResponse{DeletedCount: &count},
		Headers: CleanupExpiredAPIKeys200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// apiKeyToAPI converts a domain APIKey to the API representation.
func apiKeyToAPI(k domain.APIKey) APIKeyInfo {
	return APIKeyInfo{
		Id:          &k.ID,
		PrincipalId: &k.PrincipalID,
		Name:        &k.Name,
		KeyPrefix:   &k.KeyPrefix,
		ExpiresAt:   k.ExpiresAt,
		CreatedAt:   &k.CreatedAt,
	}
}
