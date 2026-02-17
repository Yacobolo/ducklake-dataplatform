package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
)

// macroService defines the macro operations used by the API handler.
type macroService interface {
	Create(ctx context.Context, principal string, req domain.CreateMacroRequest) (*domain.Macro, error)
	Get(ctx context.Context, name string) (*domain.Macro, error)
	List(ctx context.Context, page domain.PageRequest) ([]domain.Macro, int64, error)
	Update(ctx context.Context, principal, name string, req domain.UpdateMacroRequest) (*domain.Macro, error)
	Delete(ctx context.Context, principal, name string) error
}

// === Macros ===

// ListMacros implements the endpoint for listing SQL macros.
func (h *APIHandler) ListMacros(ctx context.Context, req ListMacrosRequestObject) (ListMacrosResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	macros, total, err := h.macros.List(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]Macro, len(macros))
	for i, m := range macros {
		data[i] = macroToAPI(m)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListMacros200JSONResponse{
		Body:    PaginatedMacros{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListMacros200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateMacro implements the endpoint for creating a new SQL macro.
func (h *APIHandler) CreateMacro(ctx context.Context, req CreateMacroRequestObject) (CreateMacroResponseObject, error) {
	domReq := domain.CreateMacroRequest{
		Name: req.Body.Name,
		Body: req.Body.Body,
	}
	if req.Body.MacroType != nil {
		domReq.MacroType = string(*req.Body.MacroType)
	}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	if req.Body.Parameters != nil {
		domReq.Parameters = *req.Body.Parameters
	}
	if req.Body.CatalogName != nil {
		domReq.CatalogName = *req.Body.CatalogName
	}
	if req.Body.ProjectName != nil {
		domReq.ProjectName = *req.Body.ProjectName
	}
	if req.Body.Visibility != nil {
		domReq.Visibility = string(*req.Body.Visibility)
	}
	if req.Body.Owner != nil {
		domReq.Owner = *req.Body.Owner
	}
	if req.Body.Properties != nil {
		domReq.Properties = map[string]string(*req.Body.Properties)
	}
	if req.Body.Tags != nil {
		domReq.Tags = *req.Body.Tags
	}
	if req.Body.Status != nil {
		domReq.Status = string(*req.Body.Status)
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.macros.Create(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateMacro403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateMacro400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateMacro409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateMacro400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateMacro201JSONResponse{
		Body:    macroToAPI(*result),
		Headers: CreateMacro201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetMacro implements the endpoint for retrieving a macro by name.
func (h *APIHandler) GetMacro(ctx context.Context, req GetMacroRequestObject) (GetMacroResponseObject, error) {
	result, err := h.macros.Get(ctx, req.MacroName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetMacro404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetMacro200JSONResponse{
		Body:    macroToAPI(*result),
		Headers: GetMacro200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateMacro implements the endpoint for updating a SQL macro.
func (h *APIHandler) UpdateMacro(ctx context.Context, req UpdateMacroRequestObject) (UpdateMacroResponseObject, error) {
	domReq := domain.UpdateMacroRequest{
		Body:        req.Body.Body,
		Description: req.Body.Description,
	}
	if req.Body.Parameters != nil {
		domReq.Parameters = *req.Body.Parameters
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.macros.Update(ctx, principal, req.MacroName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateMacro403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateMacro404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateMacro400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateMacro200JSONResponse{
		Body:    macroToAPI(*result),
		Headers: UpdateMacro200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteMacro implements the endpoint for deleting a SQL macro.
func (h *APIHandler) DeleteMacro(ctx context.Context, req DeleteMacroRequestObject) (DeleteMacroResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.macros.Delete(ctx, principal, req.MacroName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteMacro403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteMacro404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteMacro204Response{
		Headers: DeleteMacro204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Macro Mappers ===

func macroToAPI(m domain.Macro) Macro {
	ct := m.CreatedAt
	ut := m.UpdatedAt
	mt := MacroMacroType(m.MacroType)
	resp := Macro{
		Id:          &m.ID,
		Name:        &m.Name,
		MacroType:   &mt,
		Body:        &m.Body,
		Description: &m.Description,
		CreatedBy:   &m.CreatedBy,
		CreatedAt:   &ct,
		UpdatedAt:   &ut,
	}
	if m.CatalogName != "" {
		resp.CatalogName = &m.CatalogName
	}
	if m.ProjectName != "" {
		resp.ProjectName = &m.ProjectName
	}
	if m.Visibility != "" {
		v := MacroVisibility(m.Visibility)
		resp.Visibility = &v
	}
	if m.Owner != "" {
		resp.Owner = &m.Owner
	}
	if len(m.Properties) > 0 {
		props := map[string]string(m.Properties)
		resp.Properties = &props
	}
	if len(m.Tags) > 0 {
		resp.Tags = &m.Tags
	}
	if m.Status != "" {
		s := MacroStatus(m.Status)
		resp.Status = &s
	}
	if len(m.Parameters) > 0 {
		resp.Parameters = &m.Parameters
	}
	return resp
}
