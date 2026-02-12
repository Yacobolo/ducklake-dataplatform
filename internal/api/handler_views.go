package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// viewService defines the view operations used by the API handler.
type viewService interface {
	ListViews(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.ViewDetail, int64, error)
	CreateView(ctx context.Context, principal string, schemaName string, req domain.CreateViewRequest) (*domain.ViewDetail, error)
	GetView(ctx context.Context, schemaName, viewName string) (*domain.ViewDetail, error)
	UpdateView(ctx context.Context, principal string, schemaName, viewName string, req domain.UpdateViewRequest) (*domain.ViewDetail, error)
	DeleteView(ctx context.Context, principal string, schemaName, viewName string) error
}

// === Views ===

func (h *APIHandler) ListViews(ctx context.Context, req ListViewsRequestObject) (ListViewsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	views, total, err := h.views.ListViews(ctx, req.SchemaName, page)
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

func (h *APIHandler) CreateView(ctx context.Context, req CreateViewRequestObject) (CreateViewResponseObject, error) {
	domReq := domain.CreateViewRequest{
		Name:           req.Body.Name,
		ViewDefinition: req.Body.ViewDefinition,
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.views.CreateView(ctx, principal, req.SchemaName, domReq)
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

func (h *APIHandler) GetView(ctx context.Context, req GetViewRequestObject) (GetViewResponseObject, error) {
	result, err := h.views.GetView(ctx, req.SchemaName, req.ViewName)
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

func (h *APIHandler) UpdateView(ctx context.Context, req UpdateViewRequestObject) (UpdateViewResponseObject, error) {
	domReq := domain.UpdateViewRequest{}
	if req.Body.Comment != nil {
		domReq.Comment = req.Body.Comment
	}
	if req.Body.Properties != nil {
		domReq.Properties = *req.Body.Properties
	}
	if req.Body.ViewDefinition != nil {
		domReq.ViewDefinition = req.Body.ViewDefinition
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.views.UpdateView(ctx, principal, req.SchemaName, req.ViewName, domReq)
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

func (h *APIHandler) DeleteView(ctx context.Context, req DeleteViewRequestObject) (DeleteViewResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.views.DeleteView(ctx, principal, req.SchemaName, req.ViewName); err != nil {
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
