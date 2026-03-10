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
func (h *APIHandler) RegisterCatalog(ctx context.Context, request GenRegisterCatalogRequest) (GenRegisterCatalogResponse, error) {
	domReq := domain.CreateCatalogRequest{Name: request.Body.Name}
	if request.Body.MetastoreType != nil {
		domReq.MetastoreType = *request.Body.MetastoreType
	}
	if request.Body.Dsn != nil {
		domReq.DSN = *request.Body.Dsn
	}
	if request.Body.DataPath != nil {
		domReq.DataPath = *request.Body.DataPath
	}
	if request.Body.Comment != nil {
		domReq.Comment = *request.Body.Comment
	}

	result, err := h.catalogRegistration.Register(ctx, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return RegisterCatalog403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return RegisterCatalog400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return RegisterCatalog409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return RegisterCatalog400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return GenRegisterCatalog201JSONResponse{
		Body:    catalogRegistrationToAPI(*result),
		Headers: GenRegisterCatalog201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListCatalogs implements the endpoint for listing registered catalogs.
func (h *APIHandler) ListCatalogs(ctx context.Context, request GenListCatalogsRequest) (GenListCatalogsResponse, error) {
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
	return GenListCatalogs200JSONResponse{
		Body: CatalogRegistrationList{
			Catalogs:      data,
			NextPageToken: optStr(npt),
			TotalCount:    ptrI32(safeInt64ToInt32(tc)),
		},
		Headers: GenListCatalogs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetCatalogRegistration implements the endpoint for retrieving a catalog registration by name.
func (h *APIHandler) GetCatalogRegistration(ctx context.Context, request GenGetCatalogRegistrationRequest) (GenGetCatalogRegistrationResponse, error) {
	result, err := h.catalogRegistration.Get(ctx, string(request.CatalogName))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetCatalogRegistration404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenGetCatalogRegistration200JSONResponse{
		Body:    catalogRegistrationToAPI(*result),
		Headers: GenGetCatalogRegistration200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateCatalogRegistration implements the endpoint for updating a catalog registration.
func (h *APIHandler) UpdateCatalogRegistration(ctx context.Context, request GenUpdateCatalogRegistrationRequest) (GenUpdateCatalogRegistrationResponse, error) {
	domReq := domain.UpdateCatalogRegistrationRequest{
		Comment:  request.Body.Comment,
		DataPath: request.Body.DataPath,
	}

	result, err := h.catalogRegistration.Update(ctx, string(request.CatalogName), domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateCatalogRegistration403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateCatalogRegistration404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenUpdateCatalogRegistration200JSONResponse{
		Body:    catalogRegistrationToAPI(*result),
		Headers: GenUpdateCatalogRegistration200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteCatalogRegistration implements the endpoint for deleting a catalog registration.
func (h *APIHandler) DeleteCatalogRegistration(ctx context.Context, request GenDeleteCatalogRegistrationRequest) (GenDeleteCatalogRegistrationResponse, error) {
	if err := h.catalogRegistration.Delete(ctx, string(request.CatalogName)); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteCatalogRegistration403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteCatalogRegistration404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeleteCatalogRegistration204Response{
		Headers: GenDeleteCatalogRegistration204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// SetDefaultCatalog implements the endpoint for setting a catalog as the default.
func (h *APIHandler) SetDefaultCatalog(ctx context.Context, request GenSetDefaultCatalogRequest) (GenSetDefaultCatalogResponse, error) {
	result, err := h.catalogRegistration.SetDefault(ctx, string(request.CatalogName))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return SetDefaultCatalog403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return SetDefaultCatalog404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return SetDefaultCatalog400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
	systemManaged := domain.IsSystemManagedCatalog(r.Name)
	return CatalogRegistration{
		Id:            r.ID,
		Name:          r.Name,
		MetastoreType: strPtrIfNonEmpty(string(r.MetastoreType)),
		Dsn:           strPtrIfNonEmpty(r.DSN),
		DataPath:      strPtrIfNonEmpty(r.DataPath),
		Status:        strPtrIfNonEmpty(string(r.Status)),
		IsDefault:     &r.IsDefault,
		SystemManaged: &systemManaged,
		Comment:       optStr(r.Comment),
		CreatedAt:     formatTimePtr(&r.CreatedAt),
		UpdatedAt:     formatTimePtr(&r.UpdatedAt),
	}
}
