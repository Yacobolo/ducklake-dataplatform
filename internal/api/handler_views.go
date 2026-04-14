package api

import (
	"context"

	"github.com/Yacobolo/quackstack/internal/domain"
)

// viewService defines the view operations used by the API handler.
type viewService interface {
	ListViews(ctx context.Context, catalogName string, schemaName string, page domain.PageRequest) ([]domain.ViewDetail, int64, error)
	CreateView(ctx context.Context, catalogName string, principal string, schemaName string, req domain.CreateViewRequest) (*domain.ViewDetail, error)
	GetView(ctx context.Context, catalogName string, schemaName, viewName string) (*domain.ViewDetail, error)
	UpdateView(ctx context.Context, catalogName string, principal string, schemaName, viewName string, req domain.UpdateViewRequest) (*domain.ViewDetail, error)
	DeleteView(ctx context.Context, catalogName string, principal string, schemaName, viewName string) error
}

// === Views ===

// ListViews implements the endpoint for listing views in a schema.
func (h *APIHandler) ListViews(ctx context.Context, request GenListViewsRequest) (GenListViewsResponse, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	views, total, err := h.views.ListViews(ctx, string(request.CatalogName), request.SchemaName, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListViewsResponse]("listViews", err, domainErrorResponder[GenListViewsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListViewsResponse {
				return GenListViews404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]ViewDetail, len(views))
	for i, v := range views {
		data[i] = viewDetailToAPI(v)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListViews200JSONResponse{
		Body:    PaginatedViewDetails{Data: data, NextPageToken: optStr(npt)},
		Headers: GenListViews200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateView implements the endpoint for creating a new view in a schema.
func (h *APIHandler) CreateView(ctx context.Context, request GenCreateViewRequest) (GenCreateViewResponse, error) {
	domReq := domain.CreateViewRequest{
		Name:           request.Body.Name,
		ViewDefinition: request.Body.ViewDefinition,
	}
	if request.Body.Comment != nil {
		domReq.Comment = *request.Body.Comment
	}

	principal := principalFromCtx(ctx)
	result, err := h.views.CreateView(ctx, string(request.CatalogName), principal, request.SchemaName, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateViewResponse]("createView", err, domainErrorResponder[GenCreateViewResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateViewResponse { return CreateView400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenCreateViewResponse { return CreateView403JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenCreateViewResponse { return CreateView409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return CreateView400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateView201JSONResponse{
		Body:    viewDetailToAPI(*result),
		Headers: GenCreateView201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetView implements the endpoint for retrieving a view by name.
func (h *APIHandler) GetView(ctx context.Context, request GenGetViewRequest) (GenGetViewResponse, error) {
	result, err := h.views.GetView(ctx, string(request.CatalogName), request.SchemaName, request.ViewName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetViewResponse]("getView", err, domainErrorResponder[GenGetViewResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetViewResponse {
				return GenGetView404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetView200JSONResponse{
		Body:    viewDetailToAPI(*result),
		Headers: GenGetView200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateView implements the endpoint for updating a view by name.
func (h *APIHandler) UpdateView(ctx context.Context, request GenUpdateViewRequest) (GenUpdateViewResponse, error) {
	domReq := domain.UpdateViewRequest{}
	if request.Body.Comment != nil {
		domReq.Comment = request.Body.Comment
	}
	if request.Body.ViewDefinition != nil {
		domReq.ViewDefinition = request.Body.ViewDefinition
	}

	principal := principalFromCtx(ctx)
	result, err := h.views.UpdateView(ctx, string(request.CatalogName), principal, request.SchemaName, request.ViewName, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateViewResponse]("updateView", err, domainErrorResponder[GenUpdateViewResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateViewResponse { return UpdateView403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenUpdateViewResponse { return UpdateView404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateView200JSONResponse{
		Body:    viewDetailToAPI(*result),
		Headers: GenUpdateView200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteView implements the endpoint for deleting a view by name.
func (h *APIHandler) DeleteView(ctx context.Context, request GenDeleteViewRequest) (GenDeleteViewResponse, error) {
	principal := principalFromCtx(ctx)
	if err := h.views.DeleteView(ctx, string(request.CatalogName), principal, request.SchemaName, request.ViewName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteViewResponse]("deleteView", err, domainErrorResponder[GenDeleteViewResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteViewResponse { return DeleteView403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenDeleteViewResponse { return DeleteView404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteView204Response{}, nil
}
