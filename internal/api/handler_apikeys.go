package api

import (
	"context"
	"time"

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

func parseRFC3339Ptr(value string) (*time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, false
	}
	return &parsed, true
}

// CreateAPIKey implements the endpoint for creating a new API key.
func (h *APIHandler) CreateAPIKey(ctx context.Context, req GenCreateAPIKeyRequest) (GenCreateAPIKeyResponse, error) {
	domReq := domain.CreateAPIKeyRequest{PrincipalID: req.Body.PrincipalId}
	if req.Body.Name != nil {
		domReq.Name = *req.Body.Name
	}
	if req.Body.ExpiresAt != nil {
		parsed, ok := parseRFC3339Ptr(*req.Body.ExpiresAt)
		if !ok {
			return CreateAPIKey400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: "expires_at must be RFC3339"}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		domReq.ExpiresAt = parsed
	}

	rawKey, key, err := h.apiKeys.Create(ctx, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateAPIKeyResponse]("createAPIKey", err, domainErrorResponder[GenCreateAPIKeyResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateAPIKeyResponse { return CreateAPIKey400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenCreateAPIKeyResponse { return CreateAPIKey403JSONResponse{resp} },
			NotFound: func(NotFoundJSONResponse) GenCreateAPIKeyResponse {
				return CreateAPIKey400JSONResponse{badRequestErrorResponse(err)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateAPIKey201JSONResponse{
		Body: CreateAPIKeyResponse{
			Id:        key.ID,
			Key:       rawKey,
			Name:      strPtrIfNonEmpty(key.Name),
			KeyPrefix: &key.KeyPrefix,
			ExpiresAt: formatTimePtr(key.ExpiresAt),
			CreatedAt: formatTimePtr(&key.CreatedAt),
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
		if resp, ok := respondDomainErrorForOperation[GenListAPIKeysResponse]("listAPIKeys", err, domainErrorResponder[GenListAPIKeysResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListAPIKeysResponse { return ListAPIKeys403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]APIKeyInfo, len(keys))
	for i, k := range keys {
		data[i] = apiKeyToAPI(k)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListAPIKeys200JSONResponse{
		Body:    PaginatedAPIKeys{Data: data, NextPageToken: optStr(npt)},
		Headers: GenListAPIKeys200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteAPIKey implements the endpoint for deleting an API key by ID.
func (h *APIHandler) DeleteAPIKey(ctx context.Context, req GenDeleteAPIKeyRequest) (GenDeleteAPIKeyResponse, error) {
	if err := h.apiKeys.Delete(ctx, req.ApiKeyId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteAPIKeyResponse]("deleteAPIKey", err, domainErrorResponder[GenDeleteAPIKeyResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteAPIKeyResponse { return DeleteAPIKey403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenDeleteAPIKeyResponse { return DeleteAPIKey404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteAPIKey204Response{}, nil
}

// CleanupExpiredAPIKeys implements the endpoint for removing expired API keys.
func (h *APIHandler) CleanupExpiredAPIKeys(ctx context.Context, _ GenCleanupExpiredAPIKeysRequest) (GenCleanupExpiredAPIKeysResponse, error) {
	count, err := h.apiKeys.CleanupExpired(ctx)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCleanupExpiredAPIKeysResponse]("cleanupExpiredAPIKeys", err, domainErrorResponder[GenCleanupExpiredAPIKeysResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenCleanupExpiredAPIKeysResponse {
				return CleanupExpiredAPIKeys403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return CleanupExpiredAPIKeys200JSONResponse{
		Body:    CleanupAPIKeysResponse{DeletedCount: safeInt64ToInt32(count)},
		Headers: CleanupExpiredAPIKeys200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// apiKeyToAPI converts a domain APIKey to the API representation.
func apiKeyToAPI(k domain.APIKey) APIKeyInfo {
	return APIKeyInfo{
		Id:          k.ID,
		PrincipalId: k.PrincipalID,
		Name:        k.Name,
		KeyPrefix:   &k.KeyPrefix,
		ExpiresAt:   formatTimePtr(k.ExpiresAt),
		CreatedAt:   formatTimePtr(&k.CreatedAt),
	}
}
