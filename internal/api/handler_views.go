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
func (h *APIHandler) ListViews(ctx context.Context, request ListViewsRequestObject) (ListViewsResponseObject, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	views, total, err := h.views.ListViews(ctx, string(request.CatalogName), request.SchemaName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListViews404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}

	data := make([]ViewDetail, len(views))
	for i, v := range views {
		data[i] = viewDetailToAPI(v)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListViews200JSONResponse{Data: &data, NextPageToken: optStr(npt)}, nil
}

// CreateView implements the endpoint for creating a new view in a schema.
func (h *APIHandler) CreateView(ctx context.Context, request CreateViewRequestObject) (CreateViewResponseObject, error) {
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
			return CreateView403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateView400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateView409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateView400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateView201JSONResponse(viewDetailToAPI(*result)), nil
}

// GetView implements the endpoint for retrieving a view by name.
func (h *APIHandler) GetView(ctx context.Context, request GetViewRequestObject) (GetViewResponseObject, error) {
	result, err := h.views.GetView(ctx, string(request.CatalogName), request.SchemaName, request.ViewName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetView404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetView200JSONResponse(viewDetailToAPI(*result)), nil
}

// UpdateView implements the endpoint for updating a view by name.
func (h *APIHandler) UpdateView(ctx context.Context, request UpdateViewRequestObject) (UpdateViewResponseObject, error) {
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
			return UpdateView403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateView404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateView200JSONResponse(viewDetailToAPI(*result)), nil
}

// DeleteView implements the endpoint for deleting a view by name.
func (h *APIHandler) DeleteView(ctx context.Context, request DeleteViewRequestObject) (DeleteViewResponseObject, error) {
	principal := principalFromCtx(ctx)
	if err := h.views.DeleteView(ctx, string(request.CatalogName), principal, request.SchemaName, request.ViewName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteView403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteView404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteView204Response{}, nil
}
