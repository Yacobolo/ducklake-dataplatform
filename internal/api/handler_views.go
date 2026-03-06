package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenListViews404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]ViewDetail, len(views))
	for i, v := range views {
		data[i] = viewDetailToAPI(v)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListViews200JSONResponse{
		Body:    PaginatedViewDetails{Data: &data, NextPageToken: optStr(npt)},
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCreateView403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return GenCreateView400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return GenCreateView409JSONResponse{GenConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: GenConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return GenCreateView400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetView404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
	if request.Body.Properties != nil {
		domReq.Properties = *request.Body.Properties
	}
	if request.Body.ViewDefinition != nil {
		domReq.ViewDefinition = request.Body.ViewDefinition
	}

	principal := principalFromCtx(ctx)
	result, err := h.views.UpdateView(ctx, string(request.CatalogName), principal, request.SchemaName, request.ViewName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenUpdateView403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenUpdateView404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenDeleteView403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenDeleteView404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeleteView204Response{}, nil
}
