//nolint:revive // strict server interface methods are exported by generated contract.
package api

import (
	"context"
	"fmt"
	"sort"

	"duck-demo/internal/domain"
)

type productService interface {
	ListDomains(ctx context.Context, page domain.PageRequest) ([]domain.Domain, int64, error)
	GetDomain(ctx context.Context, name string) (*domain.Domain, error)
	CreateDomain(ctx context.Context, req domain.CreateDomainRequest) (*domain.Domain, error)
	UpdateDomain(ctx context.Context, name string, req domain.UpdateDomainRequest) (*domain.Domain, error)
	DeleteDomain(ctx context.Context, name string) error
	ListTeams(ctx context.Context, page domain.PageRequest) ([]domain.Team, int64, error)
	CreateTeam(ctx context.Context, req domain.CreateTeamRequest) (*domain.Team, error)
	UpdateTeam(ctx context.Context, domainName, teamName string, req domain.UpdateTeamRequest) (*domain.Team, error)
	DeleteTeam(ctx context.Context, domainName, teamName string) error
	ListProducts(ctx context.Context, filter domain.DataProductFilter) ([]domain.DataProductListItem, int64, error)
	GetProduct(ctx context.Context, slug string) (*domain.DataProductDetail, error)
	GetVersion(ctx context.Context, slug string, version int) (*domain.DataProductVersionDetail, error)
	CreateProduct(ctx context.Context, req domain.CreateDataProductRequest) (*domain.DataProductDetail, error)
	UpdateProduct(ctx context.Context, slug string, req domain.UpdateDataProductRequest) (*domain.DataProductDetail, error)
	DeleteProduct(ctx context.Context, slug string) error
	CreateVersion(ctx context.Context, slug string, req domain.CreateDataProductVersionRequest) (*domain.DataProductDetail, error)
	PublishVersion(ctx context.Context, slug string, version int) (*domain.DataProductDetail, error)
	DeprecateVersion(ctx context.Context, slug string, version int, replacementSlug *string) (*domain.DataProductDetail, error)
	RetireVersion(ctx context.Context, slug string, version int) (*domain.DataProductDetail, error)
	AddDependency(ctx context.Context, slug, dependsOnSlug string) (*domain.DataProductDetail, error)
	Subscribe(ctx context.Context, slug, principalName, eventType, channel string) (*domain.ProductSubscription, error)
	ListEvents(ctx context.Context, slug string, page domain.PageRequest) ([]domain.ProductEvent, int64, error)
	ListScorecards(ctx context.Context, page domain.PageRequest) ([]domain.ProductScorecard, int64, error)
	GetPortfolioReport(ctx context.Context) (*domain.ProductPortfolioReport, error)
}

// SetProductService attaches the product control-plane service to the API handler.
func (h *APIHandler) SetProductService(products productService) {
	h.products = products
}

func (h *APIHandler) ListProductDomains(ctx context.Context, req GenListProductDomainsRequest) (GenListProductDomainsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.products.ListDomains(ctx, page)
	if err != nil {
		if resp, ok := respondDomainError[GenListProductDomainsResponse](err, domainErrorResponder[GenListProductDomainsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListProductDomainsResponse {
				return ListProductDomains403JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListProductDomainsResponse {
				return ListProductDomains500JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]ProductDomain, len(items))
	for i := range items {
		data[i] = productDomainToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListProductDomains200JSONResponse{
		Body:    PaginatedProductDomains{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListProductDomains200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) CreateProductDomain(ctx context.Context, req GenCreateProductDomainRequest) (GenCreateProductDomainResponse, error) {
	if req.Body == nil {
		return CreateProductDomain400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.products.CreateDomain(ctx, domain.CreateDomainRequest{
		Name:        req.Body.Name,
		Description: derefString(req.Body.Description),
	})
	if err != nil {
		if resp, ok := respondDomainError[GenCreateProductDomainResponse](err, domainErrorResponder[GenCreateProductDomainResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateProductDomainResponse {
				return CreateProductDomain400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateProductDomainResponse {
				return CreateProductDomain403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateProductDomainResponse {
				return CreateProductDomain409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return CreateProductDomain201JSONResponse{
		Body:    productDomainToAPI(*item),
		Headers: CreateProductDomain201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) GetProductDomain(ctx context.Context, req GenGetProductDomainRequest) (GenGetProductDomainResponse, error) {
	item, err := h.products.GetDomain(ctx, req.DomainName)
	if err != nil {
		if resp, ok := respondDomainError[GenGetProductDomainResponse](err, domainErrorResponder[GenGetProductDomainResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetProductDomainResponse {
				return GetProductDomain404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GetProductDomain200JSONResponse{
		Body:    productDomainToAPI(*item),
		Headers: GetProductDomain200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) UpdateProductDomain(ctx context.Context, req GenUpdateProductDomainRequest) (GenUpdateProductDomainResponse, error) {
	if req.Body == nil {
		return UpdateProductDomain400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.products.UpdateDomain(ctx, req.DomainName, domain.UpdateDomainRequest{
		Description: derefString(req.Body.Description),
	})
	if err != nil {
		if resp, ok := respondDomainError[GenUpdateProductDomainResponse](err, domainErrorResponder[GenUpdateProductDomainResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateProductDomainResponse {
				return UpdateProductDomain400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateProductDomainResponse {
				return UpdateProductDomain403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateProductDomainResponse {
				return UpdateProductDomain404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateProductDomainResponse {
				return UpdateProductDomain409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return UpdateProductDomain200JSONResponse{
		Body:    productDomainToAPI(*item),
		Headers: UpdateProductDomain200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) DeleteProductDomain(ctx context.Context, req GenDeleteProductDomainRequest) (GenDeleteProductDomainResponse, error) {
	if err := h.products.DeleteDomain(ctx, req.DomainName); err != nil {
		if resp, ok := respondDomainError[GenDeleteProductDomainResponse](err, domainErrorResponder[GenDeleteProductDomainResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteProductDomainResponse {
				return DeleteProductDomain400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteProductDomainResponse {
				return DeleteProductDomain403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteProductDomainResponse {
				return DeleteProductDomain404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteProductDomainResponse {
				return DeleteProductDomain409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return DeleteProductDomain204Response{
		Headers: DeleteProductDomain204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListProductTeams(ctx context.Context, req GenListProductTeamsRequest) (GenListProductTeamsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.products.ListTeams(ctx, page)
	if err != nil {
		if resp, ok := respondDomainError[GenListProductTeamsResponse](err, domainErrorResponder[GenListProductTeamsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListProductTeamsResponse {
				return ListProductTeams403JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListProductTeamsResponse {
				return ListProductTeams500JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]ProductTeam, len(items))
	for i := range items {
		data[i] = productTeamToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListProductTeams200JSONResponse{
		Body:    PaginatedProductTeams{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListProductTeams200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) CreateProductTeam(ctx context.Context, req GenCreateProductTeamRequest) (GenCreateProductTeamResponse, error) {
	if req.Body == nil {
		return CreateProductTeam400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	domainItem, err := h.products.GetDomain(ctx, req.Body.DomainName)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateProductTeamResponse](err, domainErrorResponder[GenCreateProductTeamResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenCreateProductTeamResponse {
				return CreateProductTeam404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	item, err := h.products.CreateTeam(ctx, domain.CreateTeamRequest{
		DomainID:       domainItem.ID,
		Name:           req.Body.Name,
		ContactChannel: derefString(req.Body.ContactChannel),
	})
	if err != nil {
		if resp, ok := respondDomainError[GenCreateProductTeamResponse](err, domainErrorResponder[GenCreateProductTeamResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateProductTeamResponse {
				return CreateProductTeam400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateProductTeamResponse {
				return CreateProductTeam403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateProductTeamResponse {
				return CreateProductTeam404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateProductTeamResponse {
				return CreateProductTeam409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return CreateProductTeam201JSONResponse{
		Body:    productTeamToAPI(*item),
		Headers: CreateProductTeam201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) UpdateProductTeam(ctx context.Context, req GenUpdateProductTeamRequest) (GenUpdateProductTeamResponse, error) {
	if req.Body == nil {
		return UpdateProductTeam400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.products.UpdateTeam(ctx, req.DomainName, req.TeamName, domain.UpdateTeamRequest{
		ContactChannel: derefString(req.Body.ContactChannel),
	})
	if err != nil {
		if resp, ok := respondDomainError[GenUpdateProductTeamResponse](err, domainErrorResponder[GenUpdateProductTeamResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateProductTeamResponse {
				return UpdateProductTeam400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateProductTeamResponse {
				return UpdateProductTeam403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateProductTeamResponse {
				return UpdateProductTeam404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateProductTeamResponse {
				return UpdateProductTeam409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return UpdateProductTeam200JSONResponse{
		Body:    productTeamToAPI(*item),
		Headers: UpdateProductTeam200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) DeleteProductTeam(ctx context.Context, req GenDeleteProductTeamRequest) (GenDeleteProductTeamResponse, error) {
	if err := h.products.DeleteTeam(ctx, req.DomainName, req.TeamName); err != nil {
		if resp, ok := respondDomainError[GenDeleteProductTeamResponse](err, domainErrorResponder[GenDeleteProductTeamResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteProductTeamResponse {
				return DeleteProductTeam400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteProductTeamResponse {
				return DeleteProductTeam403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteProductTeamResponse {
				return DeleteProductTeam404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteProductTeamResponse {
				return DeleteProductTeam409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return DeleteProductTeam204Response{
		Headers: DeleteProductTeam204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListDataProducts(ctx context.Context, req GenListDataProductsRequest) (GenListDataProductsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	filter := domain.DataProductFilter{
		Query:              req.Params.Q,
		DomainName:         req.Params.Domain,
		TeamName:           req.Params.Team,
		PublicationState:   req.Params.PublicationState,
		CertificationState: req.Params.CertificationState,
		FreshnessState:     req.Params.FreshnessState,
		Page:               page,
	}
	items, total, err := h.products.ListProducts(ctx, filter)
	if err != nil {
		if resp, ok := respondDomainError[GenListDataProductsResponse](err, domainErrorResponder[GenListDataProductsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListDataProductsResponse {
				return ListDataProducts403JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListDataProductsResponse {
				return ListDataProducts500JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]DataProductListItem, len(items))
	for i := range items {
		data[i] = dataProductListItemToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListDataProducts200JSONResponse{
		Body:    PaginatedDataProducts{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListDataProducts200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) CreateDataProduct(ctx context.Context, req GenCreateDataProductRequest) (GenCreateDataProductResponse, error) {
	if req.Body == nil {
		return CreateDataProduct400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.products.CreateProduct(ctx, domainCreateDataProductRequest(req.Body))
	if err != nil {
		if resp, ok := respondDomainError[GenCreateDataProductResponse](err, domainErrorResponder[GenCreateDataProductResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateDataProductResponse {
				return CreateDataProduct400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateDataProductResponse {
				return CreateDataProduct403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateDataProductResponse {
				return CreateDataProduct409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return CreateDataProduct201JSONResponse{
		Body:    dataProductDetailToAPI(*item),
		Headers: CreateDataProduct201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) GetProductPortfolioReport(ctx context.Context, _ GenGetProductPortfolioReportRequest) (GenGetProductPortfolioReportResponse, error) {
	item, err := h.products.GetPortfolioReport(ctx)
	if err != nil {
		if resp, ok := respondDomainError[GenGetProductPortfolioReportResponse](err, domainErrorResponder[GenGetProductPortfolioReportResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetProductPortfolioReportResponse {
				return GetProductPortfolioReport403JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenGetProductPortfolioReportResponse {
				return GetProductPortfolioReport500JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GetProductPortfolioReport200JSONResponse{
		Body:    productPortfolioReportToAPI(*item),
		Headers: GetProductPortfolioReport200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListProductScorecards(ctx context.Context, req GenListProductScorecardsRequest) (GenListProductScorecardsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.products.ListScorecards(ctx, page)
	if err != nil {
		if resp, ok := respondDomainError[GenListProductScorecardsResponse](err, domainErrorResponder[GenListProductScorecardsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListProductScorecardsResponse {
				return ListProductScorecards403JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListProductScorecardsResponse {
				return ListProductScorecards500JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]ProductScorecard, len(items))
	for i := range items {
		data[i] = productScorecardToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListProductScorecards200JSONResponse{
		Body:    ProductScorecardList{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListProductScorecards200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) GetDataProduct(ctx context.Context, req GenGetDataProductRequest) (GenGetDataProductResponse, error) {
	item, err := h.products.GetProduct(ctx, req.ProductSlug)
	if err != nil {
		if resp, ok := respondDomainError[GenGetDataProductResponse](err, domainErrorResponder[GenGetDataProductResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetDataProductResponse { return GetDataProduct404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GetDataProduct200JSONResponse{
		Body:    dataProductDetailToAPI(*item),
		Headers: GetDataProduct200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) UpdateDataProduct(ctx context.Context, req GenUpdateDataProductRequest) (GenUpdateDataProductResponse, error) {
	if req.Body == nil {
		return UpdateDataProduct400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.products.UpdateProduct(ctx, req.ProductSlug, domainUpdateDataProductRequest(req.Body))
	if err != nil {
		if resp, ok := respondDomainError[GenUpdateDataProductResponse](err, domainErrorResponder[GenUpdateDataProductResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateDataProductResponse {
				return UpdateDataProduct400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateDataProductResponse {
				return UpdateDataProduct403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateDataProductResponse {
				return UpdateDataProduct404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateDataProductResponse {
				return UpdateDataProduct409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return UpdateDataProduct200JSONResponse{
		Body:    dataProductDetailToAPI(*item),
		Headers: UpdateDataProduct200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) DeleteDataProduct(ctx context.Context, req GenDeleteDataProductRequest) (GenDeleteDataProductResponse, error) {
	if err := h.products.DeleteProduct(ctx, req.ProductSlug); err != nil {
		if resp, ok := respondDomainError[GenDeleteDataProductResponse](err, domainErrorResponder[GenDeleteDataProductResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteDataProductResponse {
				return DeleteDataProduct400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteDataProductResponse {
				return DeleteDataProduct403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteDataProductResponse {
				return DeleteDataProduct404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteDataProductResponse {
				return DeleteDataProduct409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return DeleteDataProduct204Response{
		Headers: DeleteDataProduct204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListDataProductVersions(ctx context.Context, req GenListDataProductVersionsRequest) (GenListDataProductVersionsResponse, error) {
	item, err := h.products.GetProduct(ctx, req.ProductSlug)
	if err != nil {
		if resp, ok := respondDomainError[GenListDataProductVersionsResponse](err, domainErrorResponder[GenListDataProductVersionsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListDataProductVersionsResponse {
				return ListDataProductVersions404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]DataProductVersion, len(item.Versions))
	for i := range item.Versions {
		data[i] = dataProductVersionToAPI(item.Versions[i])
	}
	return ListDataProductVersions200JSONResponse{
		Body:    DataProductVersionList{Data: data},
		Headers: ListDataProductVersions200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) GetDataProductVersion(ctx context.Context, req GenGetDataProductVersionRequest) (GenGetDataProductVersionResponse, error) {
	item, err := h.products.GetVersion(ctx, req.ProductSlug, int(req.Version))
	if err != nil {
		if resp, ok := respondDomainError[GenGetDataProductVersionResponse](err, domainErrorResponder[GenGetDataProductVersionResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetDataProductVersionResponse {
				return GetDataProductVersion404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GetDataProductVersion200JSONResponse{
		Body:    dataProductVersionDetailToAPI(*item),
		Headers: GetDataProductVersion200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) CreateDataProductVersion(ctx context.Context, req GenCreateDataProductVersionRequest) (GenCreateDataProductVersionResponse, error) {
	if req.Body == nil {
		return CreateDataProductVersion400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.products.CreateVersion(ctx, req.ProductSlug, domainCreateDataProductVersionRequest(req.Body))
	if err != nil {
		if resp, ok := respondDomainError[GenCreateDataProductVersionResponse](err, domainErrorResponder[GenCreateDataProductVersionResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateDataProductVersionResponse {
				return CreateDataProductVersion400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateDataProductVersionResponse {
				return CreateDataProductVersion403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateDataProductVersionResponse {
				return CreateDataProductVersion404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateDataProductVersionResponse {
				return CreateDataProductVersion409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return CreateDataProductVersion201JSONResponse{
		Body:    dataProductDetailToAPI(*item),
		Headers: CreateDataProductVersion201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) PublishDataProductVersion(ctx context.Context, req GenPublishDataProductVersionRequest) (GenPublishDataProductVersionResponse, error) {
	version := 1
	if req.Body != nil && req.Body.Version != nil {
		version = int(*req.Body.Version)
	}
	item, err := h.products.PublishVersion(ctx, req.ProductSlug, version)
	if err != nil {
		if resp, ok := respondDomainError[GenPublishDataProductVersionResponse](err, domainErrorResponder[GenPublishDataProductVersionResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenPublishDataProductVersionResponse {
				return PublishDataProductVersion400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenPublishDataProductVersionResponse {
				return PublishDataProductVersion403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenPublishDataProductVersionResponse {
				return PublishDataProductVersion404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenPublishDataProductVersionResponse {
				return PublishDataProductVersion409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return PublishDataProductVersion200JSONResponse{
		Body:    dataProductDetailToAPI(*item),
		Headers: PublishDataProductVersion200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) DeprecateDataProductVersion(ctx context.Context, req GenDeprecateDataProductVersionRequest) (GenDeprecateDataProductVersionResponse, error) {
	version := 1
	var replacement *string
	if req.Body != nil {
		if req.Body.Version != nil {
			version = int(*req.Body.Version)
		}
		replacement = req.Body.ReplacementSlug
	}
	item, err := h.products.DeprecateVersion(ctx, req.ProductSlug, version, replacement)
	if err != nil {
		if resp, ok := respondDomainError[GenDeprecateDataProductVersionResponse](err, domainErrorResponder[GenDeprecateDataProductVersionResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeprecateDataProductVersionResponse {
				return DeprecateDataProductVersion400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeprecateDataProductVersionResponse {
				return DeprecateDataProductVersion403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeprecateDataProductVersionResponse {
				return DeprecateDataProductVersion404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeprecateDataProductVersionResponse {
				return DeprecateDataProductVersion409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return DeprecateDataProductVersion200JSONResponse{
		Body:    dataProductDetailToAPI(*item),
		Headers: DeprecateDataProductVersion200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) RetireDataProductVersion(ctx context.Context, req GenRetireDataProductVersionRequest) (GenRetireDataProductVersionResponse, error) {
	version := 1
	if req.Body != nil && req.Body.Version != nil {
		version = int(*req.Body.Version)
	}
	item, err := h.products.RetireVersion(ctx, req.ProductSlug, version)
	if err != nil {
		if resp, ok := respondDomainError[GenRetireDataProductVersionResponse](err, domainErrorResponder[GenRetireDataProductVersionResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenRetireDataProductVersionResponse {
				return RetireDataProductVersion400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenRetireDataProductVersionResponse {
				return RetireDataProductVersion403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenRetireDataProductVersionResponse {
				return RetireDataProductVersion404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenRetireDataProductVersionResponse {
				return RetireDataProductVersion409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return RetireDataProductVersion200JSONResponse{
		Body:    dataProductDetailToAPI(*item),
		Headers: RetireDataProductVersion200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) GetDataProductStatus(ctx context.Context, req GenGetDataProductStatusRequest) (GenGetDataProductStatusResponse, error) {
	item, err := h.products.GetProduct(ctx, req.ProductSlug)
	if err != nil {
		if resp, ok := respondDomainError[GenGetDataProductStatusResponse](err, domainErrorResponder[GenGetDataProductStatusResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetDataProductStatusResponse {
				return GetDataProductStatus404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	if item.Status == nil {
		return GetDataProductStatus404JSONResponse{notFoundErrorResponse(domain.ErrNotFound("status for product %q not found", req.ProductSlug))}, nil
	}
	return GetDataProductStatus200JSONResponse{
		Body:    dataProductStatusToAPI(*item.Status),
		Headers: GetDataProductStatus200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListDataProductOutputs(ctx context.Context, req GenListDataProductOutputsRequest) (GenListDataProductOutputsResponse, error) {
	item, err := h.products.GetProduct(ctx, req.ProductSlug)
	if err != nil {
		if resp, ok := respondDomainError[GenListDataProductOutputsResponse](err, domainErrorResponder[GenListDataProductOutputsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListDataProductOutputsResponse {
				return ListDataProductOutputs404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]ProductOutput, len(item.Outputs))
	for i := range item.Outputs {
		data[i] = productOutputToAPI(item.Outputs[i])
	}
	return ListDataProductOutputs200JSONResponse{
		Body:    ProductOutputList{Data: data},
		Headers: ListDataProductOutputs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListDataProductSemanticEntrypoints(ctx context.Context, req GenListDataProductSemanticEntrypointsRequest) (GenListDataProductSemanticEntrypointsResponse, error) {
	item, err := h.products.GetProduct(ctx, req.ProductSlug)
	if err != nil {
		if resp, ok := respondDomainError[GenListDataProductSemanticEntrypointsResponse](err, domainErrorResponder[GenListDataProductSemanticEntrypointsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListDataProductSemanticEntrypointsResponse {
				return ListDataProductSemanticEntrypoints404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]ProductSemanticEntrypoint, len(item.SemanticEntrypoints))
	for i := range item.SemanticEntrypoints {
		data[i] = productSemanticEntrypointToAPI(item.SemanticEntrypoints[i])
	}
	return ListDataProductSemanticEntrypoints200JSONResponse{
		Body:    ProductSemanticEntrypointList{Data: data},
		Headers: ListDataProductSemanticEntrypoints200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListDataProductDependencies(ctx context.Context, req GenListDataProductDependenciesRequest) (GenListDataProductDependenciesResponse, error) {
	item, err := h.products.GetProduct(ctx, req.ProductSlug)
	if err != nil {
		if resp, ok := respondDomainError[GenListDataProductDependenciesResponse](err, domainErrorResponder[GenListDataProductDependenciesResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListDataProductDependenciesResponse {
				return ListDataProductDependencies404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]DataProductListItem, len(item.Dependencies))
	for i := range item.Dependencies {
		data[i] = dataProductListItemToAPI(item.Dependencies[i])
	}
	return ListDataProductDependencies200JSONResponse{
		Body:    ProductDependencyList{Data: data},
		Headers: ListDataProductDependencies200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) CreateDataProductDependency(ctx context.Context, req GenCreateDataProductDependencyRequest) (GenCreateDataProductDependencyResponse, error) {
	if req.Body == nil {
		return CreateDataProductDependency400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.products.AddDependency(ctx, req.ProductSlug, req.Body.DependsOnSlug)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateDataProductDependencyResponse](err, domainErrorResponder[GenCreateDataProductDependencyResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateDataProductDependencyResponse {
				return CreateDataProductDependency400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateDataProductDependencyResponse {
				return CreateDataProductDependency403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateDataProductDependencyResponse {
				return CreateDataProductDependency404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateDataProductDependencyResponse {
				return CreateDataProductDependency409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]DataProductListItem, len(item.Dependencies))
	for i := range item.Dependencies {
		data[i] = dataProductListItemToAPI(item.Dependencies[i])
	}
	return CreateDataProductDependency201JSONResponse{
		Body:    ProductDependencyList{Data: data},
		Headers: CreateDataProductDependency201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListDataProductSubscriptions(ctx context.Context, req GenListDataProductSubscriptionsRequest) (GenListDataProductSubscriptionsResponse, error) {
	item, err := h.products.GetProduct(ctx, req.ProductSlug)
	if err != nil {
		if resp, ok := respondDomainError[GenListDataProductSubscriptionsResponse](err, domainErrorResponder[GenListDataProductSubscriptionsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListDataProductSubscriptionsResponse {
				return ListDataProductSubscriptions404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]ProductSubscription, len(item.Subscriptions))
	for i := range item.Subscriptions {
		data[i] = productSubscriptionToAPI(item.Subscriptions[i])
	}
	return ListDataProductSubscriptions200JSONResponse{
		Body:    ProductSubscriptionList{Data: data},
		Headers: ListDataProductSubscriptions200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) CreateDataProductSubscription(ctx context.Context, req GenCreateDataProductSubscriptionRequest) (GenCreateDataProductSubscriptionResponse, error) {
	if req.Body == nil {
		return CreateDataProductSubscription400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.products.Subscribe(ctx, req.ProductSlug, req.Body.PrincipalName, req.Body.EventType, productDefaultString(derefString(req.Body.Channel), "inbox"))
	if err != nil {
		if resp, ok := respondDomainError[GenCreateDataProductSubscriptionResponse](err, domainErrorResponder[GenCreateDataProductSubscriptionResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateDataProductSubscriptionResponse {
				return CreateDataProductSubscription400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateDataProductSubscriptionResponse {
				return CreateDataProductSubscription403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateDataProductSubscriptionResponse {
				return CreateDataProductSubscription404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateDataProductSubscriptionResponse {
				return CreateDataProductSubscription409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return CreateDataProductSubscription201JSONResponse{
		Body:    productSubscriptionToAPI(*item),
		Headers: CreateDataProductSubscription201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListDataProductEvents(ctx context.Context, req GenListDataProductEventsRequest) (GenListDataProductEventsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.products.ListEvents(ctx, req.ProductSlug, page)
	if err != nil {
		if resp, ok := respondDomainError[GenListDataProductEventsResponse](err, domainErrorResponder[GenListDataProductEventsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListDataProductEventsResponse {
				return ListDataProductEvents404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]ProductEvent, len(items))
	for i := range items {
		data[i] = productEventToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListDataProductEvents200JSONResponse{
		Body:    ProductEventList{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListDataProductEvents200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func productDomainToAPI(item domain.Domain) ProductDomain {
	return ProductDomain{
		Id:          item.ID,
		Name:        item.Name,
		Description: item.Description,
		CreatedAt:   formatTimePtr(&item.CreatedAt),
		UpdatedAt:   formatTimePtr(&item.UpdatedAt),
	}
}

func productTeamToAPI(item domain.Team) ProductTeam {
	return ProductTeam{
		Id:             item.ID,
		DomainId:       item.DomainID,
		Name:           item.Name,
		ContactChannel: item.ContactChannel,
		CreatedAt:      formatTimePtr(&item.CreatedAt),
		UpdatedAt:      formatTimePtr(&item.UpdatedAt),
	}
}

func dataProductToAPI(item domain.DataProduct) DataProduct {
	return DataProduct{
		Id:                  item.ID,
		Slug:                item.Slug,
		Name:                item.Name,
		Description:         item.Description,
		DomainId:            item.DomainID,
		OwnerTeamId:         item.OwnerTeamID,
		StewardPrincipal:    item.StewardPrincipal,
		ContactChannel:      item.ContactChannel,
		Visibility:          strPtrIfNonEmpty(item.Visibility),
		ConsumerAudience:    strPtrIfNonEmpty(item.ConsumerAudience),
		DocsUrl:             strPtrIfNonEmpty(item.DocsURL),
		AccessRequestPath:   strPtrIfNonEmpty(item.AccessRequestPath),
		BusinessDefinitions: recordFromStringMap(item.BusinessDefinitions),
		Contract:            productContractToAPI(item.Contract),
		Slo:                 productSLOToAPI(item.SLO),
		PublicationIntent:   strPtrIfNonEmpty(item.PublicationIntent),
		CreatedBy:           strPtrIfNonEmpty(item.CreatedBy),
		CreatedAt:           formatTimePtr(&item.CreatedAt),
		UpdatedAt:           formatTimePtr(&item.UpdatedAt),
	}
}

func dataProductVersionToAPI(item domain.DataProductVersion) DataProductVersion {
	return DataProductVersion{
		Id:                 item.ID,
		ProductId:          item.ProductID,
		Version:            safeIntToInt32(item.Version),
		ReleaseState:       item.ReleaseState,
		CompatibilityLevel: item.CompatibilityLevel,
		Contract:           productContractToAPI(item.Contract),
		Slo:                productSLOToAPI(item.SLO),
		DocsUrl:            strPtrIfNonEmpty(item.DocsURL),
		AccessRequestPath:  strPtrIfNonEmpty(item.AccessRequestPath),
		CreatedBy:          strPtrIfNonEmpty(item.CreatedBy),
		CreatedAt:          formatTimePtr(&item.CreatedAt),
	}
}

func dataProductStatusToAPI(item domain.DataProductStatus) DataProductStatus {
	return DataProductStatus{
		ProductId:              item.ProductID,
		PublicationState:       item.PublicationState,
		CertificationState:     item.CertificationState,
		FreshnessStatus:        item.FreshnessStatus,
		QualityStatus:          item.QualityStatus,
		LastSuccessfulUpdateAt: formatTimePtr(item.LastSuccessfulUpdateAt),
		FailingChecksCount:     safeInt64ToInt32(item.FailingChecksCount),
		LineageCoverage:        item.LineageCoverage,
		AdoptionMetrics:        recordFromAnyMap(item.AdoptionMetrics),
		OpenWarnings:           stringSlicePtr(item.OpenWarnings),
		ReplacementProductId:   item.ReplacementProductID,
		UpdatedAt:              formatTimePtr(&item.UpdatedAt),
	}
}

func productOutputToAPI(item domain.ProductOutput) ProductOutput {
	return ProductOutput{
		Id:               item.ID,
		ProductVersionId: item.ProductVersionID,
		AssetId:          item.AssetID,
		AssetKey:         item.AssetKey,
		AssetType:        item.AssetType,
		IsPrimary:        item.IsPrimary,
		CreatedAt:        formatTimePtr(&item.CreatedAt),
	}
}

func productSemanticEntrypointToAPI(item domain.ProductSemanticEntrypoint) ProductSemanticEntrypoint {
	return ProductSemanticEntrypoint{
		Id:               item.ID,
		ProductVersionId: item.ProductVersionID,
		SemanticModelId:  item.SemanticModelID,
		ProjectName:      item.ProjectName,
		ModelName:        item.ModelName,
		CreatedAt:        formatTimePtr(&item.CreatedAt),
	}
}

func productSubscriptionToAPI(item domain.ProductSubscription) ProductSubscription {
	return ProductSubscription{
		Id:            item.ID,
		ProductId:     item.ProductID,
		PrincipalName: item.PrincipalName,
		EventType:     item.EventType,
		Channel:       item.Channel,
		CreatedAt:     formatTimePtr(&item.CreatedAt),
	}
}

func productEventToAPI(item domain.ProductEvent) ProductEvent {
	return ProductEvent{
		Id:          item.ID,
		ProductId:   item.ProductID,
		EventType:   item.EventType,
		Title:       item.Title,
		Description: item.Description,
		Metadata:    recordFromAnyMap(item.Metadata),
		CreatedAt:   formatTimePtr(&item.CreatedAt),
	}
}

func productScorecardToAPI(item domain.ProductScorecard) ProductScorecard {
	return ProductScorecard{
		ProductId:           item.ProductID,
		ProductSlug:         item.ProductSlug,
		ProductName:         item.ProductName,
		DomainName:          item.DomainName,
		TeamName:            item.TeamName,
		PublicationState:    item.PublicationState,
		CertificationState:  item.CertificationState,
		HasOwner:            item.HasOwner,
		HasContract:         item.HasContract,
		HasSlo:              item.HasSLO,
		HasDocsOrAccessPath: item.HasDocsOrAccessPath,
		HasPrimaryOutput:    item.HasPrimaryOutput,
		HasWarnings:         item.HasWarnings,
		CompletenessPercent: safeIntToInt32(item.CompletenessPercent),
	}
}

func productAdoptionSummaryToAPI(item domain.ProductAdoptionSummary) ProductAdoptionSummary {
	return ProductAdoptionSummary{
		ProductId:               item.ProductID,
		ProductSlug:             item.ProductSlug,
		ProductName:             item.ProductName,
		DomainName:              item.DomainName,
		TeamName:                item.TeamName,
		SubscriberCount:         safeInt64ToInt32(item.SubscriberCount),
		DownstreamProductCount:  safeInt64ToInt32(item.DownstreamProductCount),
		OutputCount:             safeInt64ToInt32(item.OutputCount),
		SemanticEntrypointCount: safeInt64ToInt32(item.SemanticEntrypointCount),
		AdoptionScore:           safeInt64ToInt32(item.AdoptionScore),
	}
}

func productPortfolioGroupToAPI(item domain.ProductPortfolioGroup) ProductPortfolioGroup {
	return ProductPortfolioGroup{
		Name:                   item.Name,
		ProductCount:           safeInt64ToInt32(item.ProductCount),
		PublishedCount:         safeInt64ToInt32(item.PublishedCount),
		CertifiedCount:         safeInt64ToInt32(item.CertifiedCount),
		AverageCompletenessPct: safeIntToInt32(item.AverageCompletenessPct),
	}
}

func orphanResourceToAPI(item domain.OrphanResource) OrphanResource {
	return OrphanResource{
		ResourceType: item.ResourceType,
		ResourceId:   item.ResourceID,
		ResourceName: item.ResourceName,
	}
}

func dataProductListItemToAPI(item domain.DataProductListItem) DataProductListItem {
	result := DataProductListItem{
		Product:   dataProductToAPI(item.Product),
		Domain:    productDomainToAPI(item.Domain),
		OwnerTeam: productTeamToAPI(item.OwnerTeam),
	}
	if item.LatestVersion != nil {
		version := dataProductVersionToAPI(*item.LatestVersion)
		result.LatestVersion = &version
	}
	if item.Status != nil {
		status := dataProductStatusToAPI(*item.Status)
		result.Status = &status
	}
	if item.PrimaryOutput != nil {
		output := productOutputToAPI(*item.PrimaryOutput)
		result.PrimaryOutput = &output
	}
	return result
}

func dataProductDetailToAPI(item domain.DataProductDetail) DataProductDetail {
	versions := make([]DataProductVersion, len(item.Versions))
	for i := range item.Versions {
		versions[i] = dataProductVersionToAPI(item.Versions[i])
	}
	outputs := make([]ProductOutput, len(item.Outputs))
	for i := range item.Outputs {
		outputs[i] = productOutputToAPI(item.Outputs[i])
	}
	entrypoints := make([]ProductSemanticEntrypoint, len(item.SemanticEntrypoints))
	for i := range item.SemanticEntrypoints {
		entrypoints[i] = productSemanticEntrypointToAPI(item.SemanticEntrypoints[i])
	}
	dependencies := make([]DataProductListItem, len(item.Dependencies))
	for i := range item.Dependencies {
		dependencies[i] = dataProductListItemToAPI(item.Dependencies[i])
	}
	subscriptions := make([]ProductSubscription, len(item.Subscriptions))
	for i := range item.Subscriptions {
		subscriptions[i] = productSubscriptionToAPI(item.Subscriptions[i])
	}
	events := make([]ProductEvent, len(item.Events))
	for i := range item.Events {
		events[i] = productEventToAPI(item.Events[i])
	}
	result := DataProductDetail{
		Product:             dataProductToAPI(item.Product),
		Domain:              productDomainToAPI(item.Domain),
		OwnerTeam:           productTeamToAPI(item.OwnerTeam),
		Versions:            versions,
		Outputs:             outputs,
		SemanticEntrypoints: entrypoints,
		Dependencies:        dependencies,
		Subscriptions:       subscriptions,
		Events:              events,
	}
	if item.Status != nil {
		status := dataProductStatusToAPI(*item.Status)
		result.Status = &status
	}
	return result
}

func dataProductVersionDetailToAPI(item domain.DataProductVersionDetail) DataProductVersionDetail {
	outputs := make([]ProductOutput, len(item.Outputs))
	for i := range item.Outputs {
		outputs[i] = productOutputToAPI(item.Outputs[i])
	}
	entrypoints := make([]ProductSemanticEntrypoint, len(item.SemanticEntrypoints))
	for i := range item.SemanticEntrypoints {
		entrypoints[i] = productSemanticEntrypointToAPI(item.SemanticEntrypoints[i])
	}
	dependencies := make([]DataProductListItem, len(item.Dependencies))
	for i := range item.Dependencies {
		dependencies[i] = dataProductListItemToAPI(item.Dependencies[i])
	}
	events := make([]ProductEvent, len(item.Events))
	for i := range item.Events {
		events[i] = productEventToAPI(item.Events[i])
	}
	result := DataProductVersionDetail{
		Product:             dataProductToAPI(item.Product),
		Domain:              productDomainToAPI(item.Domain),
		OwnerTeam:           productTeamToAPI(item.OwnerTeam),
		Version:             dataProductVersionToAPI(item.Version),
		Outputs:             outputs,
		SemanticEntrypoints: entrypoints,
		Dependencies:        dependencies,
		Events:              events,
	}
	if item.Status != nil {
		status := dataProductStatusToAPI(*item.Status)
		result.Status = &status
	}
	return result
}

func productPortfolioReportToAPI(item domain.ProductPortfolioReport) ProductPortfolioReport {
	topUsed := make([]ProductAdoptionSummary, len(item.TopUsed))
	for i := range item.TopUsed {
		topUsed[i] = productAdoptionSummaryToAPI(item.TopUsed[i])
	}
	leastAdopted := make([]ProductAdoptionSummary, len(item.LeastAdopted))
	for i := range item.LeastAdopted {
		leastAdopted[i] = productAdoptionSummaryToAPI(item.LeastAdopted[i])
	}
	highBlastRadius := make([]ProductAdoptionSummary, len(item.HighBlastRadius))
	for i := range item.HighBlastRadius {
		highBlastRadius[i] = productAdoptionSummaryToAPI(item.HighBlastRadius[i])
	}
	domainScorecards := make([]ProductPortfolioGroup, len(item.DomainScorecards))
	for i := range item.DomainScorecards {
		domainScorecards[i] = productPortfolioGroupToAPI(item.DomainScorecards[i])
	}
	teamScorecards := make([]ProductPortfolioGroup, len(item.TeamScorecards))
	for i := range item.TeamScorecards {
		teamScorecards[i] = productPortfolioGroupToAPI(item.TeamScorecards[i])
	}
	orphanAssets := make([]OrphanResource, len(item.OrphanAssets))
	for i := range item.OrphanAssets {
		orphanAssets[i] = orphanResourceToAPI(item.OrphanAssets[i])
	}
	orphanSemanticModels := make([]OrphanResource, len(item.OrphanSemanticModels))
	for i := range item.OrphanSemanticModels {
		orphanSemanticModels[i] = orphanResourceToAPI(item.OrphanSemanticModels[i])
	}
	return ProductPortfolioReport{
		TopUsed:              topUsed,
		LeastAdopted:         leastAdopted,
		HighBlastRadius:      highBlastRadius,
		DomainScorecards:     domainScorecards,
		TeamScorecards:       teamScorecards,
		OrphanAssets:         orphanAssets,
		OrphanSemanticModels: orphanSemanticModels,
	}
}

func productContractToAPI(item domain.ProductContract) *ProductContract {
	if isZeroProductContract(item) {
		return nil
	}
	result := ProductContract{
		DataGrain:            strPtrIfNonEmpty(item.DataGrain),
		PrimaryKeys:          stringSlicePtr(item.PrimaryKeys),
		JoinKeys:             stringSlicePtr(item.JoinKeys),
		Dimensions:           stringSlicePtr(item.Dimensions),
		Measures:             stringSlicePtr(item.Measures),
		RetentionWindow:      strPtrIfNonEmpty(item.RetentionWindow),
		UpdateCadence:        strPtrIfNonEmpty(item.UpdateCadence),
		QualityExpectations:  stringSlicePtr(item.QualityExpectations),
		BreakingChangePolicy: strPtrIfNonEmpty(item.BreakingChangePolicy),
		SampleQueries:        stringSlicePtr(item.SampleQueries),
	}
	return &result
}

func productSLOToAPI(item domain.ProductSLO) *ProductSLO {
	if item == (domain.ProductSLO{}) {
		return nil
	}
	result := ProductSLO{
		FreshnessSlo: strPtrIfNonEmpty(item.FreshnessSLO),
		LatencySlo:   strPtrIfNonEmpty(item.LatencySLO),
	}
	return &result
}

func domainCreateDataProductRequest(body *GenCreateDataProductJSONBody) domain.CreateDataProductRequest {
	req := domain.CreateDataProductRequest{
		Slug:                body.Slug,
		Name:                body.Name,
		Description:         derefString(body.Description),
		DomainName:          body.DomainName,
		TeamName:            body.TeamName,
		StewardPrincipal:    body.StewardPrincipal,
		ContactChannel:      body.ContactChannel,
		Visibility:          derefString(body.Visibility),
		ConsumerAudience:    derefString(body.ConsumerAudience),
		DocsURL:             derefString(body.DocsUrl),
		AccessRequestPath:   derefString(body.AccessRequestPath),
		BusinessDefinitions: stringMapFromRecord(body.BusinessDefinitions),
		Contract:            domainProductContract(body.Contract),
		SLO:                 domainProductSLO(body.Slo),
		SemanticModelRefs:   cloneStringSlicePtr(body.SemanticModelRefs),
		CreatedBy:           productDefaultString(derefString(body.CreatedBy), "system"),
	}
	if body.PrimaryAssetKey != nil && *body.PrimaryAssetKey != "" {
		req.PrimaryAssetKey = body.PrimaryAssetKey
	}
	return req
}

func domainUpdateDataProductRequest(body *GenUpdateDataProductJSONBody) domain.UpdateDataProductRequest {
	return domain.UpdateDataProductRequest{
		Name:                body.Name,
		Description:         derefString(body.Description),
		DomainName:          body.DomainName,
		TeamName:            body.TeamName,
		StewardPrincipal:    body.StewardPrincipal,
		ContactChannel:      body.ContactChannel,
		Visibility:          derefString(body.Visibility),
		ConsumerAudience:    derefString(body.ConsumerAudience),
		DocsURL:             derefString(body.DocsUrl),
		AccessRequestPath:   derefString(body.AccessRequestPath),
		BusinessDefinitions: stringMapFromRecord(body.BusinessDefinitions),
		Contract:            domainProductContract(body.Contract),
		SLO:                 domainProductSLO(body.Slo),
		PublicationIntent:   derefString(body.PublicationIntent),
	}
}

func domainCreateDataProductVersionRequest(body *GenCreateDataProductVersionJSONBody) domain.CreateDataProductVersionRequest {
	return domain.CreateDataProductVersionRequest{
		CompatibilityLevel: derefString(body.CompatibilityLevel),
		Contract:           domainProductContract(body.Contract),
		SLO:                domainProductSLO(body.Slo),
		DocsURL:            derefString(body.DocsUrl),
		AccessRequestPath:  derefString(body.AccessRequestPath),
		OutputAssetKeys:    cloneStringSlicePtr(body.OutputAssetKeys),
		SemanticModelRefs:  cloneStringSlicePtr(body.SemanticModelRefs),
		CreatedBy:          derefString(body.CreatedBy),
	}
}

func domainProductContract(item *ProductContract) domain.ProductContract {
	if item == nil {
		return domain.ProductContract{}
	}
	return domain.ProductContract{
		DataGrain:            derefString(item.DataGrain),
		PrimaryKeys:          cloneStringSlicePtr(item.PrimaryKeys),
		JoinKeys:             cloneStringSlicePtr(item.JoinKeys),
		Dimensions:           cloneStringSlicePtr(item.Dimensions),
		Measures:             cloneStringSlicePtr(item.Measures),
		RetentionWindow:      derefString(item.RetentionWindow),
		UpdateCadence:        derefString(item.UpdateCadence),
		QualityExpectations:  cloneStringSlicePtr(item.QualityExpectations),
		BreakingChangePolicy: derefString(item.BreakingChangePolicy),
		SampleQueries:        cloneStringSlicePtr(item.SampleQueries),
	}
}

func domainProductSLO(item *ProductSLO) domain.ProductSLO {
	if item == nil {
		return domain.ProductSLO{}
	}
	return domain.ProductSLO{
		FreshnessSLO: derefString(item.FreshnessSlo),
		LatencySLO:   derefString(item.LatencySlo),
	}
}

func recordFromStringMap(values map[string]string) *Record {
	if len(values) == 0 {
		return nil
	}
	record := Record{}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		record[key] = values[key]
	}
	return &record
}

func recordFromAnyMap(values map[string]any) *Record {
	if len(values) == 0 {
		return nil
	}
	record := Record{}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		record[key] = values[key]
	}
	return &record
}

func stringMapFromRecord(values *Record) map[string]string {
	if values == nil || len(*values) == 0 {
		return nil
	}
	result := make(map[string]string, len(*values))
	for key, value := range *values {
		if text, ok := value.(string); ok {
			result[key] = text
			continue
		}
		result[key] = fmt.Sprint(value)
	}
	return result
}

func stringSlicePtr(values []string) *[]string {
	if len(values) == 0 {
		return nil
	}
	cloned := append([]string(nil), values...)
	return &cloned
}

func cloneStringSlicePtr(values *[]string) []string {
	if values == nil {
		return nil
	}
	return append([]string(nil), (*values)...)
}

func isZeroProductContract(item domain.ProductContract) bool {
	return item.DataGrain == "" &&
		len(item.PrimaryKeys) == 0 &&
		len(item.JoinKeys) == 0 &&
		len(item.Dimensions) == 0 &&
		len(item.Measures) == 0 &&
		item.RetentionWindow == "" &&
		item.UpdateCadence == "" &&
		len(item.QualityExpectations) == 0 &&
		item.BreakingChangePolicy == "" &&
		len(item.SampleQueries) == 0
}

func productDefaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
