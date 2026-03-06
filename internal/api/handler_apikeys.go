package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
)

// apiKeyService defines the API key management operations used by the API handler.
type apiKeyService interface {
	Create(ctx context.Context, req domain.CreateAPIKeyRequest) (string, *domain.APIKey, error)
	List(ctx context.Context, principalID *string, page domain.PageRequest) ([]domain.APIKey, int64, error)
	Delete(ctx context.Context, id string) error
	CleanupExpired(ctx context.Context) (int64, error)
}

// === API Keys ===

// CreateAPIKey implements the endpoint for creating a new API key.
func (h *APIHandler) CreateAPIKey(ctx context.Context, req GenCreateAPIKeyRequest) (GenCreateAPIKeyResponse, error) {
	rawKey, key, err := h.apiKeys.Create(ctx, domain.CreateAPIKeyRequest{
		PrincipalID: req.Body.PrincipalId,
		Name:        req.Body.Name,
		ExpiresAt:   req.Body.ExpiresAt,
	})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return GenCreateAPIKey400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCreateAPIKey403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenCreateAPIKey201JSONResponse{
		Body: CreateAPIKeyResponse{
			Id:        &key.ID,
			Key:       &rawKey,
			Name:      &key.Name,
			KeyPrefix: &key.KeyPrefix,
			ExpiresAt: key.ExpiresAt,
			CreatedAt: &key.CreatedAt,
		},
		Headers: GenCreateAPIKey201ResponseHeaders{
			XRateLimitLimit:     defaultRateLimitLimit,
			XRateLimitRemaining: defaultRateLimitRemaining,
			XRateLimitReset:     defaultRateLimitReset,
		},
	}, nil
}

// ListAPIKeys implements the endpoint for listing API keys for a principal.
func (h *APIHandler) ListAPIKeys(ctx context.Context, req GenListAPIKeysRequest) (GenListAPIKeysResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	keys, total, err := h.apiKeys.List(ctx, req.Params.PrincipalId, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenListAPIKeys403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	data := make([]APIKeyInfo, len(keys))
	for i, k := range keys {
		data[i] = apiKeyToAPI(k)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListAPIKeys200JSONResponse{
		Body:    PaginatedAPIKeys{Data: &data, NextPageToken: optStr(npt)},
		Headers: GenListAPIKeys200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteAPIKey implements the endpoint for deleting an API key by ID.
func (h *APIHandler) DeleteAPIKey(ctx context.Context, req GenDeleteAPIKeyRequest) (GenDeleteAPIKeyResponse, error) {
	if err := h.apiKeys.Delete(ctx, req.ApiKeyId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenDeleteAPIKey404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeleteAPIKey204Response{}, nil
}

// CleanupExpiredAPIKeys implements the endpoint for removing expired API keys.
func (h *APIHandler) CleanupExpiredAPIKeys(ctx context.Context, _ GenCleanupExpiredAPIKeysRequest) (GenCleanupExpiredAPIKeysResponse, error) {
	count, err := h.apiKeys.CleanupExpired(ctx)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCleanupExpiredAPIKeys403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenCleanupExpiredAPIKeys201JSONResponse{
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
