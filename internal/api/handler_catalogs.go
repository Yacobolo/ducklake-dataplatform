package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
)

// catalogRegistrationService defines the catalog registration operations used by the API handler.
type catalogRegistrationService interface {
	Register(ctx context.Context, req domain.CreateCatalogRequest) (*domain.CatalogRegistration, error)
	List(ctx context.Context, page domain.PageRequest) ([]domain.CatalogRegistration, int64, error)
	Get(ctx context.Context, name string) (*domain.CatalogRegistration, error)
	Update(ctx context.Context, name string, req domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error)
	Delete(ctx context.Context, name string) error
	SetDefault(ctx context.Context, name string) (*domain.CatalogRegistration, error)
}

// === Catalog Registration ===

// RegisterCatalog implements the endpoint for registering a new catalog.
func (h *APIHandler) RegisterCatalog(ctx context.Context, request RegisterCatalogRequestObject) (RegisterCatalogResponseObject, error) {
	domReq := domain.CreateCatalogRequest{
		Name:          request.Body.Name,
		MetastoreType: string(request.Body.MetastoreType),
		DSN:           request.Body.Dsn,
		DataPath:      request.Body.DataPath,
	}
	if request.Body.Comment != nil {
		domReq.Comment = *request.Body.Comment
	}

	result, err := h.catalogRegistration.Register(ctx, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return RegisterCatalog403JSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: RegisterCatalog403ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return RegisterCatalog400JSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: RegisterCatalog400ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return RegisterCatalog409JSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: RegisterCatalog409ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		default:
			return RegisterCatalog400JSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: RegisterCatalog400ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		}
	}
	return RegisterCatalog201JSONResponse{
		Body:    catalogRegistrationToAPI(*result),
		Headers: RegisterCatalog201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListCatalogs implements the endpoint for listing registered catalogs.
func (h *APIHandler) ListCatalogs(ctx context.Context, request ListCatalogsRequestObject) (ListCatalogsResponseObject, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	catalogs, total, err := h.catalogRegistration.List(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]CatalogRegistration, len(catalogs))
	for i, c := range catalogs {
		data[i] = catalogRegistrationToAPI(c)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	tc := total
	return ListCatalogs200JSONResponse{
		Body: CatalogRegistrationList{
			Catalogs:      &data,
			NextPageToken: optStr(npt),
			TotalCount:    &tc,
		},
		Headers: ListCatalogs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetCatalogRegistration implements the endpoint for retrieving a catalog registration by name.
func (h *APIHandler) GetCatalogRegistration(ctx context.Context, request GetCatalogRegistrationRequestObject) (GetCatalogRegistrationResponseObject, error) {
	result, err := h.catalogRegistration.Get(ctx, string(request.CatalogName))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetCatalogRegistration404JSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GetCatalogRegistration404ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		default:
			return nil, err
		}
	}
	return GetCatalogRegistration200JSONResponse{
		Body:    catalogRegistrationToAPI(*result),
		Headers: GetCatalogRegistration200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateCatalogRegistration implements the endpoint for updating a catalog registration.
func (h *APIHandler) UpdateCatalogRegistration(ctx context.Context, request UpdateCatalogRegistrationRequestObject) (UpdateCatalogRegistrationResponseObject, error) {
	domReq := domain.UpdateCatalogRegistrationRequest{
		Comment:  request.Body.Comment,
		DataPath: request.Body.DataPath,
		DSN:      request.Body.Dsn,
	}

	result, err := h.catalogRegistration.Update(ctx, string(request.CatalogName), domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateCatalogRegistration403JSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: UpdateCatalogRegistration403ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateCatalogRegistration404JSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: UpdateCatalogRegistration404ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		default:
			return nil, err
		}
	}
	return UpdateCatalogRegistration200JSONResponse{
		Body:    catalogRegistrationToAPI(*result),
		Headers: UpdateCatalogRegistration200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteCatalogRegistration implements the endpoint for deleting a catalog registration.
func (h *APIHandler) DeleteCatalogRegistration(ctx context.Context, request DeleteCatalogRegistrationRequestObject) (DeleteCatalogRegistrationResponseObject, error) {
	if err := h.catalogRegistration.Delete(ctx, string(request.CatalogName)); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteCatalogRegistration403JSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: DeleteCatalogRegistration403ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteCatalogRegistration404JSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: DeleteCatalogRegistration404ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		default:
			return nil, err
		}
	}
	return DeleteCatalogRegistration204Response{
		Headers: DeleteCatalogRegistration204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// SetDefaultCatalog implements the endpoint for setting a catalog as the default.
func (h *APIHandler) SetDefaultCatalog(ctx context.Context, request SetDefaultCatalogRequestObject) (SetDefaultCatalogResponseObject, error) {
	result, err := h.catalogRegistration.SetDefault(ctx, string(request.CatalogName))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return SetDefaultCatalog403JSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: SetDefaultCatalog403ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return SetDefaultCatalog404JSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: SetDefaultCatalog404ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return SetDefaultCatalog403JSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: SetDefaultCatalog403ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		default:
			return nil, err
		}
	}
	return SetDefaultCatalog200JSONResponse{
		Body:    catalogRegistrationToAPI(*result),
		Headers: SetDefaultCatalog200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// catalogRegistrationToAPI converts a domain CatalogRegistration to the API type.
func catalogRegistrationToAPI(r domain.CatalogRegistration) CatalogRegistration {
	ct := r.CreatedAt
	ut := r.UpdatedAt
	return CatalogRegistration{
		Id:            &r.ID,
		Name:          r.Name,
		MetastoreType: CatalogRegistrationMetastoreType(r.MetastoreType),
		Dsn:           r.DSN,
		DataPath:      r.DataPath,
		Status:        CatalogRegistrationStatus(r.Status),
		StatusMessage: optStr(r.StatusMessage),
		IsDefault:     &r.IsDefault,
		Comment:       optStr(r.Comment),
		CreatedAt:     &ct,
		UpdatedAt:     &ut,
	}
}
