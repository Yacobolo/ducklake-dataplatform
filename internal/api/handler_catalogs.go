package api

import (
	"context"

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
		domReq.MetastoreType = string(*request.Body.MetastoreType)
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
		if resp, ok := respondDomainErrorForOperation[GenRegisterCatalogResponse]("registerCatalog", err, domainErrorResponder[GenRegisterCatalogResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenRegisterCatalogResponse {
				return RegisterCatalog400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenRegisterCatalogResponse {
				return RegisterCatalog403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenRegisterCatalogResponse {
				return RegisterCatalog409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return RegisterCatalog400JSONResponse{badRequestErrorResponse(err)}, nil
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
		if resp, ok := respondDomainErrorForOperation[GenListCatalogsResponse]("listCatalogs", err, domainErrorResponder[GenListCatalogsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListCatalogsResponse {
				return GenListCatalogs403JSONResponse{GenForbiddenJSONResponse(resp)}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListCatalogsResponse {
				return GenListCatalogs500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
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

// UpdateCatalogRegistration implements the endpoint for updating a catalog registration.
func (h *APIHandler) UpdateCatalogRegistration(ctx context.Context, request GenUpdateCatalogRegistrationRequest) (GenUpdateCatalogRegistrationResponse, error) {
	domReq := domain.UpdateCatalogRegistrationRequest{
		Comment:  request.Body.Comment,
		DataPath: request.Body.DataPath,
	}

	result, err := h.catalogRegistration.Update(ctx, string(request.CatalogName), domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateCatalogRegistrationResponse]("updateCatalogRegistration", err, domainErrorResponder[GenUpdateCatalogRegistrationResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateCatalogRegistrationResponse {
				return UpdateCatalogRegistration403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateCatalogRegistrationResponse {
				return UpdateCatalogRegistration404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateCatalogRegistration200JSONResponse{
		Body:    catalogRegistrationToAPI(*result),
		Headers: GenUpdateCatalogRegistration200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteCatalogRegistration implements the endpoint for deleting a catalog registration.
func (h *APIHandler) DeleteCatalogRegistration(ctx context.Context, request GenDeleteCatalogRegistrationRequest) (GenDeleteCatalogRegistrationResponse, error) {
	if err := h.catalogRegistration.Delete(ctx, string(request.CatalogName)); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteCatalogRegistrationResponse]("deleteCatalogRegistration", err, domainErrorResponder[GenDeleteCatalogRegistrationResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteCatalogRegistrationResponse {
				return DeleteCatalogRegistration403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteCatalogRegistrationResponse {
				return DeleteCatalogRegistration404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteCatalogRegistration204Response{
		Headers: GenDeleteCatalogRegistration204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// SetDefaultCatalog implements the endpoint for setting a catalog as the default.
func (h *APIHandler) SetDefaultCatalog(ctx context.Context, request GenSetDefaultCatalogRequest) (GenSetDefaultCatalogResponse, error) {
	result, err := h.catalogRegistration.SetDefault(ctx, string(request.CatalogName))
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenSetDefaultCatalogResponse]("setDefaultCatalog", err, domainErrorResponder[GenSetDefaultCatalogResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenSetDefaultCatalogResponse {
				return SetDefaultCatalog400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenSetDefaultCatalogResponse {
				return SetDefaultCatalog403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenSetDefaultCatalogResponse {
				return SetDefaultCatalog404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
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
		MetastoreType: ptrMetastoreType(string(r.MetastoreType)),
		Dsn:           strPtrIfNonEmpty(r.DSN),
		DataPath:      strPtrIfNonEmpty(r.DataPath),
		Status:        ptrCatalogStatus(string(r.Status)),
		IsDefault:     &r.IsDefault,
		SystemManaged: &systemManaged,
		Comment:       optStr(r.Comment),
		CreatedAt:     formatTimePtr(&r.CreatedAt),
		UpdatedAt:     formatTimePtr(&r.UpdatedAt),
	}
}
