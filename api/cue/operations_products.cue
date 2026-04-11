package api

// Authored product operations.

#productsTag: "Products"

#plainProductOperation: #genericOperationSpec & {
	wrapped: false
}

#domainNamePathParameter: #pathStringParameter & {
	#name: "domain_name"
}

#teamNamePathParameter: #pathStringParameter & {
	#name: "team_name"
}

#productSlugPathParameter: #pathStringParameter & {
	#name: "product_slug"
}

#versionPathParameter: #pathInt32Parameter & {
	#name: "version"
}

#paginationOnlyParameters: #paginationParameters

#productDomainPathParameters: [
	#domainNamePathParameter,
]

#productTeamPathParameters: [
	#domainNamePathParameter,
	#teamNamePathParameter,
]

#productTeamListParameters: [
	#domainNamePathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#productSlugPathParameters: [
	#productSlugPathParameter,
]

#productSlugVersionPathParameters: [
	#productSlugPathParameter,
	#versionPathParameter,
]

#productEventsParameters: [
	#productSlugPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#queryStringFilterParameter: {
	#name: string

	name:    #name
	in:      "query"
	explode: false
	schema: {
		type: "string"
	}
}

#searchDataProductParameters: [
	#queryStringFilterParameter & {
		#name: "q"
	},
	#queryStringFilterParameter & {
		#name: "domain"
	},
	#queryStringFilterParameter & {
		#name: "team"
	},
	#queryStringFilterParameter & {
		#name: "publication_state"
	},
	#queryStringFilterParameter & {
		#name: "certification_state"
	},
	#queryStringFilterParameter & {
		#name: "freshness_state"
	},
	#paginationParameters[0],
	#paginationParameters[1],
]

#productOps: [
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProductDomains"
		path:         "/product-domains"
		summary:      "List product domains"
		returns:      "PaginatedProductDomains"
		error_family: "standard"
		params:       #paginationOnlyParameters
	},
	#plainProductOperation & {
		kind:           "response"
		method:         "post"
		op:             "createProductDomain"
		path:           "/product-domains"
		summary:        "Create product domain"
		returns:        "ProductDomain"
		success_status: 201
		error_family:   "mutating_conflict"
		body_ref:       "CreateProductDomainRequest"
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProductDomain"
		path:         "/product-domains/{domain_name}"
		summary:      "Get product domain"
		returns:      "ProductDomain"
		error_family: "lookup"
		params:       #productDomainPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateProductDomain"
		path:         "/product-domains/{domain_name}"
		summary:      "Update product domain"
		returns:      "ProductDomain"
		error_family: "resource"
		params:       #productDomainPathParameters
		body_ref:     "UpdateProductDomainRequest"
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProductDomain"
		path:         "/product-domains/{domain_name}"
		summary:      "Delete product domain"
		error_family: "resource"
		params:       #productDomainPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProductTeams"
		path:         "/product-domains/{domain_name}/teams"
		summary:      "List product teams"
		returns:      "PaginatedProductTeams"
		error_family: "standard"
		params:       #productTeamListParameters
	},
	#plainProductOperation & {
		kind:           "response"
		method:         "post"
		op:             "createProductTeam"
		path:           "/product-domains/{domain_name}/teams"
		summary:        "Create product team"
		returns:        "ProductTeam"
		success_status: 201
		error_family:   "resource"
		params:         #productDomainPathParameters
		body_ref:       "CreateProductTeamRequest"
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProductTeam"
		path:         "/product-domains/{domain_name}/teams/{team_name}"
		summary:      "Get product team"
		returns:      "ProductTeam"
		error_family: "resource"
		params:       #productTeamPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateProductTeam"
		path:         "/product-domains/{domain_name}/teams/{team_name}"
		summary:      "Update product team"
		returns:      "ProductTeam"
		error_family: "resource"
		params:       #productTeamPathParameters
		body_ref:     "UpdateProductTeamRequest"
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteProductTeam"
		path:         "/product-domains/{domain_name}/teams/{team_name}"
		summary:      "Delete product team"
		error_family: "resource"
		params:       #productTeamPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listDataProducts"
		path:         "/data-products"
		summary:      "List data products"
		returns:      "PaginatedDataProducts"
		error_family: "standard"
		params:       #searchDataProductParameters
	},
	#plainProductOperation & {
		kind:           "response"
		method:         "post"
		op:             "createDataProduct"
		path:           "/data-products"
		summary:        "Create data product"
		returns:        "DataProductDetail"
		success_status: 201
		error_family:   "mutating_conflict"
		body_ref:       "CreateDataProductRequest"
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listProductScorecards"
		path:         "/data-products/scorecards"
		summary:      "List product scorecards"
		returns:      "ProductScorecardList"
		error_family: "standard"
		params:       #paginationOnlyParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "getProductPortfolioReport"
		path:         "/data-products/portfolio"
		summary:      "Get product portfolio report"
		returns:      "ProductPortfolioReport"
		error_family: "standard"
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "getDataProduct"
		path:         "/data-products/{product_slug}"
		summary:      "Get data product"
		returns:      "DataProductDetail"
		error_family: "lookup"
		params:       #productSlugPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateDataProduct"
		path:         "/data-products/{product_slug}"
		summary:      "Update data product"
		returns:      "DataProductDetail"
		error_family: "resource"
		params:       #productSlugPathParameters
		body_ref:     "UpdateDataProductRequest"
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteDataProduct"
		path:         "/data-products/{product_slug}"
		summary:      "Delete data product"
		error_family: "resource"
		params:       #productSlugPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listDataProductVersions"
		path:         "/data-products/{product_slug}/versions"
		summary:      "List data product versions"
		returns:      "DataProductVersionList"
		error_family: "lookup"
		params:       #productSlugPathParameters
	},
	#plainProductOperation & {
		kind:           "response"
		method:         "post"
		op:             "createDataProductVersion"
		path:           "/data-products/{product_slug}/versions"
		summary:        "Create data product version"
		returns:        "DataProductDetail"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #productSlugPathParameters
		body_ref:       "CreateDataProductVersionRequest"
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "getDataProductVersion"
		path:         "/data-products/{product_slug}/versions/{version}"
		summary:      "Get data product version"
		returns:      "DataProductVersionDetail"
		error_family: "lookup"
		params:       #productSlugVersionPathParameters
	},
	#plainProductOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteDataProductVersion"
		path:         "/data-products/{product_slug}/versions/{version}"
		summary:      "Delete data product version"
		error_family: "resource"
		params:       #productSlugVersionPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "post"
		op:           "publishDataProductVersion"
		path:         "/data-products/{product_slug}/versions/{version}/publications"
		summary:      "Publish data product version"
		returns:      "DataProductDetail"
		error_family: "resource"
		params:       #productSlugVersionPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "post"
		op:           "deprecateDataProductVersion"
		path:         "/data-products/{product_slug}/versions/{version}/deprecations"
		summary:      "Deprecate data product version"
		returns:      "DataProductDetail"
		error_family: "resource"
		params:       #productSlugVersionPathParameters
		body_ref:     "DeprecateProductVersionRequest"
		body_required: false
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "post"
		op:           "retireDataProductVersion"
		path:         "/data-products/{product_slug}/versions/{version}/retirements"
		summary:      "Retire data product version"
		returns:      "DataProductDetail"
		error_family: "resource"
		params:       #productSlugVersionPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "getDataProductStatus"
		path:         "/data-products/{product_slug}/status"
		summary:      "Get data product status"
		returns:      "DataProductStatus"
		error_family: "lookup"
		params:       #productSlugPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listDataProductOutputs"
		path:         "/data-products/{product_slug}/outputs"
		summary:      "List data product outputs"
		returns:      "ProductOutputList"
		error_family: "lookup"
		params:       #productSlugPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listDataProductSemanticEntrypoints"
		path:         "/data-products/{product_slug}/semantic-entrypoints"
		summary:      "List data product semantic entrypoints"
		returns:      "ProductSemanticEntrypointList"
		error_family: "lookup"
		params:       #productSlugPathParameters
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listDataProductDependencies"
		path:         "/data-products/{product_slug}/dependencies"
		summary:      "List data product dependencies"
		returns:      "ProductDependencyList"
		error_family: "lookup"
		params:       #productSlugPathParameters
	},
	#plainProductOperation & {
		kind:           "response"
		method:         "post"
		op:             "createDataProductDependency"
		path:           "/data-products/{product_slug}/dependencies"
		summary:        "Create data product dependency"
		returns:        "ProductDependencyList"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #productSlugPathParameters
		body_ref:       "CreateProductDependencyRequest"
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listDataProductSubscriptions"
		path:         "/data-products/{product_slug}/subscriptions"
		summary:      "List data product subscriptions"
		returns:      "ProductSubscriptionList"
		error_family: "lookup"
		params:       #productSlugPathParameters
	},
	#plainProductOperation & {
		kind:           "response"
		method:         "post"
		op:             "createDataProductSubscription"
		path:           "/data-products/{product_slug}/subscriptions"
		summary:        "Create data product subscription"
		returns:        "ProductSubscription"
		success_status: 201
		error_family:   "resource_conflict"
		params:         #productSlugPathParameters
		body_ref:       "CreateProductSubscriptionRequest"
		body_description: "Request payload"
	},
	#plainProductOperation & {
		kind:         "response"
		method:       "get"
		op:           "listDataProductEvents"
		path:         "/data-products/{product_slug}/events"
		summary:      "List data product events"
		returns:      "ProductEventList"
		error_family: "lookup"
		params:       #productEventsParameters
	},
]

endpoints_products: [
	for op in #productOps {
		(#endpointFromGenericOperation & {
			tag:  #productsTag
			spec: op
		}).endpoint
	},
]
