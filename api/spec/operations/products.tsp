using TypeSpec.Http;

namespace Duck;

@route("/product-domains")
@get
@tag("Products")
@summary("List product domains")
op listProductDomains(@query max_results?: int32, @query page_token?: string): Ok<PaginatedProductDomains> | StandardErrorResponses;

@route("/product-domains")
@post
@tag("Products")
@summary("Create product domain")
op createProductDomain(@body body: CreateProductDomainRequest): Created<ProductDomain> | MutatingErrorResponses | ConflictResponse;

@route("/product-domains/{domainName}")
@get
@tag("Products")
@summary("Get product domain")
op getProductDomain(domainName: string): Ok<ProductDomain> | LookupErrorResponses;

@route("/product-domains/{domainName}")
@patch(#{ implicitOptionality: true })
@tag("Products")
@summary("Update product domain")
op updateProductDomain(domainName: string, @body body: UpdateProductDomainRequest): Ok<ProductDomain> | ResourceErrorResponses | BadRequestResponse;

@route("/product-domains/{domainName}")
@delete
@tag("Products")
@summary("Delete product domain")
op deleteProductDomain(domainName: string): NoContent | ResourceErrorResponses;

@route("/product-teams")
@get
@tag("Products")
@summary("List product teams")
op listProductTeams(@query domain_name?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedProductTeams> | StandardErrorResponses;

@route("/product-domains/{domainName}/teams")
@post
@tag("Products")
@summary("Create product team")
op createProductTeam(domainName: string, @body body: CreateProductTeamRequest): Created<ProductTeam> | ValidationResourceErrorResponses;

@route("/product-domains/{domainName}/teams/{teamName}")
@get
@tag("Products")
@summary("Get product team")
op getProductTeam(domainName: string, teamName: string): Ok<ProductTeam> | ResourceErrorResponses;

@route("/product-domains/{domainName}/teams/{teamName}")
@patch(#{ implicitOptionality: true })
@tag("Products")
@summary("Update product team")
op updateProductTeam(domainName: string, teamName: string, @body body: UpdateProductTeamRequest): Ok<ProductTeam> | ResourceErrorResponses | BadRequestResponse;

@route("/product-domains/{domainName}/teams/{teamName}")
@delete
@tag("Products")
@summary("Delete product team")
op deleteProductTeam(domainName: string, teamName: string): NoContent | ResourceErrorResponses;

@route("/data-products")
@get
@tag("Products")
@summary("List data products")
op listDataProducts(
  @query q?: string,
  @query domain?: string,
  @query team?: string,
  @query publication_state?: string,
  @query certification_state?: string,
  @query freshness_state?: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedDataProducts> | StandardErrorResponses;

@route("/data-products")
@post
@tag("Products")
@summary("Create data product")
op createDataProduct(@body body: CreateDataProductRequest): Created<DataProductDetail> | MutatingErrorResponses | ConflictResponse | BadRequestResponse;

@route("/data-products/scorecards")
@get
@tag("Products")
@summary("List product scorecards")
op listProductScorecards(@query max_results?: int32, @query page_token?: string): Ok<ProductScorecardList> | StandardErrorResponses;

@route("/data-products/portfolio")
@get
@tag("Products")
@summary("Get product portfolio report")
op getProductPortfolioReport(): Ok<ProductPortfolioReport> | StandardErrorResponses;

@route("/data-products/{productSlug}")
@get
@tag("Products")
@summary("Get data product")
op getDataProduct(productSlug: string): Ok<DataProductDetail> | LookupErrorResponses;

@route("/data-products/{productSlug}")
@patch(#{ implicitOptionality: true })
@tag("Products")
@summary("Update data product")
op updateDataProduct(productSlug: string, @body body: UpdateDataProductRequest): Ok<DataProductDetail> | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{productSlug}")
@delete
@tag("Products")
@summary("Delete data product")
op deleteDataProduct(productSlug: string): NoContent | ResourceErrorResponses;

@route("/data-products/{productSlug}/versions")
@get
@tag("Products")
@summary("List data product versions")
op listDataProductVersions(productSlug: string): Ok<DataProductVersionList> | LookupErrorResponses;

@route("/data-products/{productSlug}/versions/{version}")
@get
@tag("Products")
@summary("Get data product version")
op getDataProductVersion(productSlug: string, version: int32): Ok<DataProductVersionDetail> | LookupErrorResponses;

@route("/data-products/{productSlug}/versions/{version}")
@delete
@tag("Products")
@summary("Delete data product version")
op deleteDataProductVersion(productSlug: string, version: int32): NoContent | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{productSlug}/versions")
@post
@tag("Products")
@summary("Create data product version")
op createDataProductVersion(productSlug: string, @body body: CreateDataProductVersionRequest): Created<DataProductDetail> | ResourceErrorResponses | BadRequestResponse | ConflictResponse;

@route("/data-products/{productSlug}/versions/{version}:publish")
@post
@tag("Products")
@summary("Publish data product version")
op publishDataProductVersion(productSlug: string, version: int32): Ok<DataProductDetail> | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{productSlug}/versions/{version}:deprecate")
@post
@tag("Products")
@summary("Deprecate data product version")
op deprecateDataProductVersion(productSlug: string, version: int32, @body body?: DeprecateProductVersionRequest): Ok<DataProductDetail> | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{productSlug}/versions/{version}:retire")
@post
@tag("Products")
@summary("Retire data product version")
op retireDataProductVersion(productSlug: string, version: int32): Ok<DataProductDetail> | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{productSlug}/status")
@get
@tag("Products")
@summary("Get data product status")
op getDataProductStatus(productSlug: string): Ok<DataProductStatus> | LookupErrorResponses;

@route("/data-products/{productSlug}/outputs")
@get
@tag("Products")
@summary("List data product outputs")
op listDataProductOutputs(productSlug: string): Ok<ProductOutputList> | LookupErrorResponses;

@route("/data-products/{productSlug}/semantic-entrypoints")
@get
@tag("Products")
@summary("List data product semantic entrypoints")
op listDataProductSemanticEntrypoints(productSlug: string): Ok<ProductSemanticEntrypointList> | LookupErrorResponses;

@route("/data-products/{productSlug}/dependencies")
@get
@tag("Products")
@summary("List data product dependencies")
op listDataProductDependencies(productSlug: string): Ok<ProductDependencyList> | LookupErrorResponses;

@route("/data-products/{productSlug}/dependencies")
@post
@tag("Products")
@summary("Create data product dependency")
op createDataProductDependency(productSlug: string, @body body: CreateProductDependencyRequest): Created<ProductDependencyList> | ResourceErrorResponses | BadRequestResponse | ConflictResponse;

@route("/data-products/{productSlug}/subscriptions")
@get
@tag("Products")
@summary("List data product subscriptions")
op listDataProductSubscriptions(productSlug: string): Ok<ProductSubscriptionList> | LookupErrorResponses;

@route("/data-products/{productSlug}/subscriptions")
@post
@tag("Products")
@summary("Create data product subscription")
op createDataProductSubscription(productSlug: string, @body body: CreateProductSubscriptionRequest): Created<ProductSubscription> | ResourceErrorResponses | BadRequestResponse | ConflictResponse;

@route("/data-products/{productSlug}/events")
@get
@tag("Products")
@summary("List data product events")
op listDataProductEvents(productSlug: string, @query max_results?: int32, @query page_token?: string): Ok<ProductEventList> | LookupErrorResponses;
