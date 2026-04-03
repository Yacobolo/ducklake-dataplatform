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

@route("/product-domains/{domain_name}")
@get
@tag("Products")
@summary("Get product domain")
op getProductDomain(domain_name: string): Ok<ProductDomain> | LookupErrorResponses;

@route("/product-domains/{domain_name}")
@patch(#{ implicitOptionality: true })
@tag("Products")
@summary("Update product domain")
op updateProductDomain(domain_name: string, @body body: UpdateProductDomainRequest): Ok<ProductDomain> | ResourceErrorResponses | BadRequestResponse;

@route("/product-domains/{domain_name}")
@delete
@tag("Products")
@summary("Delete product domain")
op deleteProductDomain(domain_name: string): NoContent | ResourceErrorResponses;

@route("/product-domains/teams")
@get
@tag("Products")
@summary("List product teams")
op listProductTeams(@query domain_name?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedProductTeams> | StandardErrorResponses;

@route("/product-domains/{domain_name}/teams")
@post
@tag("Products")
@summary("Create product team")
op createProductTeam(domain_name: string, @body body: CreateProductTeamRequest): Created<ProductTeam> | ValidationResourceErrorResponses;

@route("/product-domains/{domain_name}/teams/{team_name}")
@get
@tag("Products")
@summary("Get product team")
op getProductTeam(domain_name: string, team_name: string): Ok<ProductTeam> | ResourceErrorResponses;

@route("/product-domains/{domain_name}/teams/{team_name}")
@patch(#{ implicitOptionality: true })
@tag("Products")
@summary("Update product team")
op updateProductTeam(domain_name: string, team_name: string, @body body: UpdateProductTeamRequest): Ok<ProductTeam> | ResourceErrorResponses | BadRequestResponse;

@route("/product-domains/{domain_name}/teams/{team_name}")
@delete
@tag("Products")
@summary("Delete product team")
op deleteProductTeam(domain_name: string, team_name: string): NoContent | ResourceErrorResponses;

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

@route("/data-products/{product_slug}")
@get
@tag("Products")
@summary("Get data product")
op getDataProduct(product_slug: string): Ok<DataProductDetail> | LookupErrorResponses;

@route("/data-products/{product_slug}")
@patch(#{ implicitOptionality: true })
@tag("Products")
@summary("Update data product")
op updateDataProduct(product_slug: string, @body body: UpdateDataProductRequest): Ok<DataProductDetail> | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{product_slug}")
@delete
@tag("Products")
@summary("Delete data product")
op deleteDataProduct(product_slug: string): NoContent | ResourceErrorResponses;

@route("/data-products/{product_slug}/versions")
@get
@tag("Products")
@summary("List data product versions")
op listDataProductVersions(product_slug: string): Ok<DataProductVersionList> | LookupErrorResponses;

@route("/data-products/{product_slug}/versions/{version}")
@get
@tag("Products")
@summary("Get data product version")
op getDataProductVersion(product_slug: string, version: int32): Ok<DataProductVersionDetail> | LookupErrorResponses;

@route("/data-products/{product_slug}/versions/{version}")
@delete
@tag("Products")
@summary("Delete data product version")
op deleteDataProductVersion(product_slug: string, version: int32): NoContent | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{product_slug}/versions")
@post
@tag("Products")
@summary("Create data product version")
op createDataProductVersion(product_slug: string, @body body: CreateDataProductVersionRequest): Created<DataProductDetail> | ResourceErrorResponses | BadRequestResponse | ConflictResponse;

@route("/data-products/{product_slug}/versions/{version}/publications")
@post
@tag("Products")
@summary("Publish data product version")
op publishDataProductVersion(product_slug: string, version: int32): Ok<DataProductDetail> | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{product_slug}/versions/{version}/deprecations")
@post
@tag("Products")
@summary("Deprecate data product version")
op deprecateDataProductVersion(product_slug: string, version: int32, @body body?: DeprecateProductVersionRequest): Ok<DataProductDetail> | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{product_slug}/versions/{version}/retirements")
@post
@tag("Products")
@summary("Retire data product version")
op retireDataProductVersion(product_slug: string, version: int32): Ok<DataProductDetail> | ResourceErrorResponses | BadRequestResponse;

@route("/data-products/{product_slug}/status")
@get
@tag("Products")
@summary("Get data product status")
op getDataProductStatus(product_slug: string): Ok<DataProductStatus> | LookupErrorResponses;

@route("/data-products/{product_slug}/outputs")
@get
@tag("Products")
@summary("List data product outputs")
op listDataProductOutputs(product_slug: string): Ok<ProductOutputList> | LookupErrorResponses;

@route("/data-products/{product_slug}/semantic-entrypoints")
@get
@tag("Products")
@summary("List data product semantic entrypoints")
op listDataProductSemanticEntrypoints(product_slug: string): Ok<ProductSemanticEntrypointList> | LookupErrorResponses;

@route("/data-products/{product_slug}/dependencies")
@get
@tag("Products")
@summary("List data product dependencies")
op listDataProductDependencies(product_slug: string): Ok<ProductDependencyList> | LookupErrorResponses;

@route("/data-products/{product_slug}/dependencies")
@post
@tag("Products")
@summary("Create data product dependency")
op createDataProductDependency(product_slug: string, @body body: CreateProductDependencyRequest): Created<ProductDependencyList> | ResourceErrorResponses | BadRequestResponse | ConflictResponse;

@route("/data-products/{product_slug}/subscriptions")
@get
@tag("Products")
@summary("List data product subscriptions")
op listDataProductSubscriptions(product_slug: string): Ok<ProductSubscriptionList> | LookupErrorResponses;

@route("/data-products/{product_slug}/subscriptions")
@post
@tag("Products")
@summary("Create data product subscription")
op createDataProductSubscription(product_slug: string, @body body: CreateProductSubscriptionRequest): Created<ProductSubscription> | ResourceErrorResponses | BadRequestResponse | ConflictResponse;

@route("/data-products/{product_slug}/events")
@get
@tag("Products")
@summary("List data product events")
op listDataProductEvents(product_slug: string, @query max_results?: int32, @query page_token?: string): Ok<ProductEventList> | LookupErrorResponses;
