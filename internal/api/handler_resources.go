package api

import (
	"context"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/resourceref"
)

type resourceAccessService interface {
	ListRecent(ctx context.Context, principal domain.ContextPrincipal, limit int) ([]domain.ResourceAccessEvent, error)
}

type savedResourceService interface {
	Save(ctx context.Context, principal domain.ContextPrincipal, resource domain.ResourceRef) error
	Unsave(ctx context.Context, principal domain.ContextPrincipal, resourceType string, resourceKey string) error
	ListSaved(ctx context.Context, principal domain.ContextPrincipal, limit int) ([]domain.SavedResource, error)
}

func (h *APIHandler) ListRecentResources(ctx context.Context, req GenListRecentResourcesRequest) (GenListRecentResourcesResponse, error) {
	if isNilService(h.resourceAccess) {
		return ListRecentResources500JSONResponse{internalErrorResponse(domain.ErrNotImplemented("resource access service is not configured"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	limit := 0
	if req.Params.MaxResults != nil {
		limit = int(*req.Params.MaxResults)
	}

	items, err := h.resourceAccess.ListRecent(ctx, cp, limit)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListRecentResourcesResponse]("listRecentResources", err, domainErrorResponder[GenListRecentResourcesResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListRecentResourcesResponse { return ListRecentResources400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenListRecentResourcesResponse { return ListRecentResources403JSONResponse{resp} },
			Internal:   func(resp InternalErrorJSONResponse) GenListRecentResourcesResponse { return ListRecentResources500JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]RecentResource, len(items))
	for i := range items {
		data[i] = recentResourceToAPI(items[i])
	}

	return ListRecentResources200JSONResponse{
		Body:    PaginatedRecentResources{Data: data},
		Headers: ListRecentResources200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListSavedResources(ctx context.Context, req GenListSavedResourcesRequest) (GenListSavedResourcesResponse, error) {
	if isNilService(h.savedResources) {
		return ListSavedResources500JSONResponse{internalErrorResponse(domain.ErrNotImplemented("saved resource service is not configured"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	limit := 0
	if req.Params.MaxResults != nil {
		limit = int(*req.Params.MaxResults)
	}

	items, err := h.savedResources.ListSaved(ctx, cp, limit)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListSavedResourcesResponse]("listSavedResources", err, domainErrorResponder[GenListSavedResourcesResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListSavedResourcesResponse { return ListSavedResources400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenListSavedResourcesResponse { return ListSavedResources403JSONResponse{resp} },
			Internal:   func(resp InternalErrorJSONResponse) GenListSavedResourcesResponse { return ListSavedResources500JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]SavedResource, len(items))
	for i := range items {
		data[i] = savedResourceToAPI(items[i])
	}

	return ListSavedResources200JSONResponse{
		Body:    PaginatedSavedResources{Data: data},
		Headers: ListSavedResources200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) CreateSavedResource(ctx context.Context, req GenCreateSavedResourceRequest) (GenCreateSavedResourceResponse, error) {
	if isNilService(h.savedResources) {
		return CreateSavedResource500JSONResponse{internalErrorResponse(domain.ErrNotImplemented("saved resource service is not configured"))}, nil
	}

	resource, err := normalizeAPIResourceRef(domain.ResourceRef{
		ResourceType: req.Body.ResourceType,
		ResourceKey:  req.Body.ResourceKey,
		DisplayName:  valueOrEmpty(req.Body.DisplayName),
		ResourcePath: valueOrEmpty(req.Body.ResourcePath),
		Section:      valueOrEmpty(req.Body.Section),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateSavedResourceResponse]("createSavedResource", err, domainErrorResponder[GenCreateSavedResourceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateSavedResourceResponse {
				return CreateSavedResource400JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.savedResources.Save(ctx, cp, resource); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateSavedResourceResponse]("createSavedResource", err, domainErrorResponder[GenCreateSavedResourceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateSavedResourceResponse { return CreateSavedResource400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenCreateSavedResourceResponse { return CreateSavedResource403JSONResponse{resp} },
			Internal:   func(resp InternalErrorJSONResponse) GenCreateSavedResourceResponse { return CreateSavedResource500JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return CreateSavedResource201JSONResponse{
		Body:    savedResourceToAPI(domain.SavedResource{ResourceRef: resource, SavedAt: time.Now().UTC()}),
		Headers: CreateSavedResource201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) DeleteSavedResource(ctx context.Context, req GenDeleteSavedResourceRequest) (GenDeleteSavedResourceResponse, error) {
	if isNilService(h.savedResources) {
		return DeleteSavedResource500JSONResponse{internalErrorResponse(domain.ErrNotImplemented("saved resource service is not configured"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.savedResources.Unsave(ctx, cp, req.ResourceType, req.ResourceKey); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteSavedResourceResponse]("deleteSavedResource", err, domainErrorResponder[GenDeleteSavedResourceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteSavedResourceResponse { return DeleteSavedResource400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenDeleteSavedResourceResponse { return DeleteSavedResource403JSONResponse{resp} },
			Internal:   func(resp InternalErrorJSONResponse) GenDeleteSavedResourceResponse { return DeleteSavedResource500JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return DeleteSavedResource204Response{
		Headers: DeleteSavedResource204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func normalizeAPIResourceRef(resource domain.ResourceRef) (domain.ResourceRef, error) {
	normalized, err := resourceref.Normalize(resource)
	if err != nil {
		return domain.ResourceRef{}, err
	}
	if !resourceref.IsRecentResource(normalized) {
		return domain.ResourceRef{}, domain.ErrValidation("resource must be a UUID-backed resource")
	}
	return normalized, nil
}

func recentResourceToAPI(item domain.ResourceAccessEvent) RecentResource {
	return RecentResource{
		ResourceType: item.ResourceType,
		ResourceKey:  item.ResourceKey,
		DisplayName:  item.DisplayName,
		ResourcePath: optStr(item.ResourcePath),
		Href:         optStr(item.Href),
		Section:      optStr(item.Section),
		AccessedAt:   formatTimePtr(&item.AccessedAt),
	}
}

func savedResourceToAPI(item domain.SavedResource) SavedResource {
	return SavedResource{
		ResourceType:   item.ResourceType,
		ResourceKey:    item.ResourceKey,
		DisplayName:    item.DisplayName,
		ResourcePath:   optStr(item.ResourcePath),
		Href:           optStr(item.Href),
		Section:        optStr(item.Section),
		SavedAt:        formatTimePtr(&item.SavedAt),
		LastAccessedAt: formatTimePtr(item.LastAccessedAt),
	}
}

func valueOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
