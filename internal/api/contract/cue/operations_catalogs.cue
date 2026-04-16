package api

// Authored catalog operations.

#catalogsTag: "Catalogs"
#catalogsPath: "/catalogs"

#catalogNamePathParameter: #pathStringParameter & {
	#name: "catalog_name"
}

#schemaNamePathParameter: #pathStringParameter & {
	#name: "schema_name"
}

#tableNamePathParameter: #pathStringParameter & {
	#name: "table_name"
}

#columnNamePathParameter: #pathStringParameter & {
	#name: "column_name"
}

#viewNamePathParameter: #pathStringParameter & {
	#name: "view_name"
}

#volumeNamePathParameter: #pathStringParameter & {
	#name: "volume_name"
}

#requiredQueryStringParameter: {
	#name: string

	name:     #name
	in:       "query"
	required: true
	explode:  false
	schema: {
		type: "string"
	}
}

#queryBoolParameter: {
	#name: string

	name:    #name
	in:      "query"
	explode: false
	schema: {
		type: "boolean"
	}
}

#limitQueryParameter: #queryInt32Parameter & {
	#name: "limit"
}

#entityTypeQueryParameter: #queryStringParameter & {
	#name: "entity_type"
}

#catalogQueryParameter: #queryStringParameter & {
	#name: "catalog"
}

#queryQueryParameter: #requiredQueryStringParameter & {
	#name: "query"
}

#typeQueryParameter: #queryStringParameter & {
	#name: "type"
}

#schemaNameQueryParameter: #queryStringParameter & {
	#name: "schema_name"
}

#tableNameQueryParameter: #queryStringParameter & {
	#name: "table_name"
}

#catalogPathParameters: [
	#catalogNamePathParameter,
]

#catalogSchemaPathParameters: [
	#catalogNamePathParameter,
	#schemaNamePathParameter,
]

#catalogSchemaTablePathParameters: [
	#catalogNamePathParameter,
	#schemaNamePathParameter,
	#tableNamePathParameter,
]

#catalogSchemaViewPathParameters: [
	#catalogNamePathParameter,
	#schemaNamePathParameter,
	#viewNamePathParameter,
]

#catalogSchemaVolumePathParameters: [
	#catalogNamePathParameter,
	#schemaNamePathParameter,
	#volumeNamePathParameter,
]

#searchCatalogParameters: [
	#queryQueryParameter,
	#typeQueryParameter,
	#catalogQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#listCatalogsParameters: #paginationParameters

#catalogHistoryParameters: [
	#catalogNamePathParameter,
	#entityTypeQueryParameter,
	#schemaNameQueryParameter,
	#tableNameQueryParameter,
	#limitQueryParameter,
]

#catalogSchemaPaginationParameters: [
	#catalogNamePathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#catalogSchemaPathPaginationParameters: [
	#catalogNamePathParameter,
	#schemaNamePathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#catalogSchemaTablePaginationParameters: [
	#catalogNamePathParameter,
	#schemaNamePathParameter,
	#tableNamePathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#catalogSchemaColumnPathParameters: [
	#catalogNamePathParameter,
	#schemaNamePathParameter,
	#tableNamePathParameter,
	#columnNamePathParameter,
]

#deleteSchemaParameters: [
	#catalogNamePathParameter,
	#schemaNamePathParameter,
	#queryBoolParameter & {
		#name: "force"
	},
]

#privilegeCreateSchemaOnCatalog: {
	mode: "privilege"
	checks: [{
		securable_type:      "catalog"
		privilege:           "CREATE_SCHEMA"
		securable_id_source: "catalog_name_param"
	}]
}

#privilegeCreateSchemaOnSchema: {
	mode: "privilege"
	checks: [{
		securable_type:      "schema"
		privilege:           "CREATE_SCHEMA"
		securable_id_source: "runtime_resolved_object_id"
	}]
}

#privilegeCreateTableOnSchema: {
	mode: "privilege"
	checks: [{
		securable_type:      "schema"
		privilege:           "CREATE_TABLE"
		securable_id_source: "runtime_resolved_object_id"
	}]
}

#privilegeCreateTableOnTable: {
	mode: "privilege"
	checks: [{
		securable_type:      "table"
		privilege:           "CREATE_TABLE"
		securable_id_source: "runtime_resolved_object_id"
	}]
}

#privilegeCreateVolumeOnCatalog: {
	mode: "privilege"
	checks: [{
		securable_type:      "catalog"
		privilege:           "CREATE_VOLUME"
		securable_id_source: "catalog_sentinel"
	}]
}

#privilegeCreateVolumeOnVolume: {
	mode: "privilege"
	checks: [{
		securable_type:      "volume"
		privilege:           "CREATE_VOLUME"
		securable_id_source: "runtime_resolved_object_id"
	}]
}

#privilegeInsertOnTable: {
	mode: "privilege"
	checks: [{
		securable_type:      "table"
		privilege:           "INSERT"
		securable_id_source: "runtime_resolved_object_id"
	}]
}

#catalogOps: [
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "searchCatalog"
		path:         #catalogsPath + "/search"
		summary:      "Search catalog"
		cli:          "catalog search"
		returns:      "PaginatedSearchResults"
		error_family: "standard"
		params:       #searchCatalogParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "post"
		op:              "registerCatalog"
		path:            #catalogsPath
		summary:         "Register catalog"
		cli:             "catalog registrations create"
		returns:         "CatalogRegistration"
		success_status:  201
		error_family:    "mutating"
		body_ref:        "CreateCatalogRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listCatalogs"
		path:         #catalogsPath
		summary:      "List catalog registrations"
		description:  "Lists registered catalogs and returns a paginated catalog registration view for management clients."
		cli:          "catalog registrations list"
		returns:      "CatalogRegistrationList"
		error_family: "guarded_read"
		params:       #listCatalogsParameters
		authz_default: false
		authz:        #adminOnlyAuthz
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getCatalog"
		path:         #catalogsPath + "/{catalog_name}"
		summary:      "Get catalog"
		cli:          "catalog registrations get"
		returns:      "CatalogRegistration"
		error_family: "resource"
		params:       #catalogPathParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "patch"
		op:              "updateCatalogRegistration"
		path:            #catalogsPath + "/{catalog_name}"
		summary:         "Update catalog registration"
		cli:             "catalog registrations update"
		returns:         "CatalogRegistration"
		error_family:    "resource"
		params:          #catalogPathParameters
		body_ref:        "UpdateCatalogRegistrationRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteCatalogRegistration"
		path:         #catalogsPath + "/{catalog_name}"
		summary:      "Delete catalog registration"
		cli:          "catalog registrations delete"
		error_family: "resource"
		params:       #catalogPathParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "put"
		op:              "setDefaultCatalog"
		path:            #catalogsPath + "/{catalog_name}/default"
		summary:         "Set default catalog"
		cli:             "catalog registrations set-default"
		returns:         "CatalogRegistration"
		error_family:    "mutating"
		params:          #catalogPathParameters
		body_ref:        "SetDefaultCatalogRequest"
		body_description: "Request payload"
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getMetastoreSummary"
		path:         #catalogsPath + "/{catalog_name}/metastore/summary"
		summary:      "Get metastore summary"
		cli:          "catalog metastore summary"
		returns:      "MetastoreSummary"
		error_family: "resource"
		params:       #catalogPathParameters
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getCatalogVersionSummary"
		path:         #catalogsPath + "/{catalog_name}/version-summary"
		summary:      "Get catalog version summary"
		returns:      "CatalogVersionSummary"
		wrapped:      false
		error_family: "resource"
		params:       #catalogPathParameters
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listCatalogHistory"
		path:         #catalogsPath + "/{catalog_name}/history"
		summary:      "List catalog history"
		returns:      "CatalogHistoryResponse"
		wrapped:      false
		error_family: "resource"
		params:       #catalogHistoryParameters
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listSchemas"
		path:         #catalogsPath + "/{catalog_name}/schemas"
		summary:      "List schemas"
		cli:          "catalog schemas list"
		returns:      "PaginatedSchemaDetails"
		error_family: "resource"
		params:       #catalogSchemaPaginationParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "post"
		op:              "createSchema"
		path:            #catalogsPath + "/{catalog_name}/schemas"
		summary:         "Create schema"
		cli:             "catalog schemas create"
		returns:         "SchemaDetail"
		success_status:  201
		error_family:    "mutating"
		params:          #catalogPathParameters
		body_ref:        "CreateSchemaRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeCreateSchemaOnCatalog
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getSchema"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}"
		summary:      "Get schema"
		cli:          "catalog schemas get"
		returns:      "SchemaDetail"
		error_family: "resource"
		params:       #catalogSchemaPathParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "patch"
		op:              "updateSchema"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}"
		summary:         "Update schema"
		cli:             "catalog schemas update"
		returns:         "SchemaDetail"
		error_family:    "mutating"
		params:          #catalogSchemaPathParameters
		body_ref:        "UpdateSchemaRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeCreateSchemaOnSchema
	},
	#genericOperationSpec & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteSchema"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}"
		summary:      "Delete schema"
		cli:          "catalog schemas delete"
		error_family: "mutating"
		params:       #deleteSchemaParameters
		authz_default: false
		authz:        #privilegeCreateSchemaOnSchema
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listTables"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables"
		summary:      "List tables"
		cli:          "catalog tables list"
		returns:      "PaginatedTableDetails"
		error_family: "resource"
		params:       #catalogSchemaPathPaginationParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "post"
		op:              "createTable"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables"
		summary:         "Create table"
		cli:             "catalog tables create"
		returns:         "TableDetail"
		success_status:  201
		error_family:    "mutating"
		params:          #catalogSchemaPathParameters
		body_ref:        "CreateTableRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeCreateTableOnSchema
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getTable"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}"
		summary:      "Get table"
		cli:          "catalog tables get"
		returns:      "TableDetail"
		error_family: "resource"
		params:       #catalogSchemaTablePathParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "patch"
		op:              "updateTable"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}"
		summary:         "Update table"
		cli:             "catalog tables update"
		returns:         "TableDetail"
		error_family:    "mutating"
		params:          #catalogSchemaTablePathParameters
		body_ref:        "UpdateTableRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeCreateTableOnTable
	},
	#genericOperationSpec & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteTable"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}"
		summary:      "Delete table"
		cli:          "catalog tables delete"
		error_family: "mutating"
		params:       #catalogSchemaTablePathParameters
		authz_default: false
		authz:        #privilegeCreateTableOnTable
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listTableColumns"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}/columns"
		summary:      "List table columns"
		cli:          "catalog columns list"
		returns:      "PaginatedColumnDetails"
		error_family: "resource"
		params:       #catalogSchemaTablePaginationParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "patch"
		op:              "updateColumn"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}/columns/{column_name}"
		summary:         "Update column"
		cli:             "catalog columns update"
		returns:         "ColumnDetail"
		error_family:    "mutating"
		params:          #catalogSchemaColumnPathParameters
		body_ref:        "UpdateColumnRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeCreateTableOnTable
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "post"
		op:           "profileTable"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}/profiles"
		summary:      "Profile table"
		cli:          "catalog tables profile"
		returns:      "TableStatistics"
		error_family: "mutating"
		params:       #catalogSchemaTablePathParameters
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "createManifest"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}/manifest"
		summary:      "Get table manifest"
		cli:          "catalog tables manifest get"
		returns:      "ManifestResponse"
		error_family: "resource"
		params:       #catalogSchemaTablePathParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "post"
		op:              "createUploadUrl"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}/upload-urls"
		summary:         "Create upload URL"
		cli:             "ingestion upload-url"
		returns:         "UploadUrlResponse"
		error_family:    "mutating"
		params:          #catalogSchemaTablePathParameters
		body_ref:        "UploadUrlRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeInsertOnTable
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "post"
		op:              "commitTableIngestion"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}/ingestion-commits"
		summary:         "Commit table ingestion"
		cli:             "ingestion commit"
		returns:         "IngestionResult"
		error_family:    "mutating"
		params:          #catalogSchemaTablePathParameters
		body_ref:        "CommitIngestionRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeInsertOnTable
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "post"
		op:              "loadTableExternalFiles"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/tables/{table_name}/ingestion-loads"
		summary:         "Load table external files"
		cli:             "ingestion load"
		returns:         "IngestionResult"
		error_family:    "mutating"
		params:          #catalogSchemaTablePathParameters
		body_ref:        "LoadExternalRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeInsertOnTable
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listViews"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/views"
		summary:      "List views"
		cli:          "catalog views list"
		returns:      "PaginatedViewDetails"
		error_family: "resource"
		params:       #catalogSchemaPathPaginationParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "post"
		op:              "createView"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/views"
		summary:         "Create view"
		cli:             "catalog views create"
		returns:         "ViewDetail"
		success_status:  201
		error_family:    "mutating"
		params:          #catalogSchemaPathParameters
		body_ref:        "CreateViewRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeCreateTableOnSchema
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getView"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/views/{view_name}"
		summary:      "Get view"
		cli:          "catalog views get"
		returns:      "ViewDetail"
		error_family: "resource"
		params:       #catalogSchemaViewPathParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "patch"
		op:              "updateView"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/views/{view_name}"
		summary:         "Update view"
		cli:             "catalog views update"
		returns:         "ViewDetail"
		error_family:    "mutating"
		params:          #catalogSchemaViewPathParameters
		body_ref:        "UpdateViewRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeCreateTableOnSchema
	},
	#genericOperationSpec & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteView"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/views/{view_name}"
		summary:      "Delete view"
		cli:          "catalog views delete"
		error_family: "mutating"
		params:       #catalogSchemaViewPathParameters
		authz_default: false
		authz:        #privilegeCreateTableOnSchema
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "listVolumes"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/volumes"
		summary:      "List volumes"
		cli:          "catalog volumes list"
		returns:      "PaginatedVolumes"
		error_family: "resource"
		params:       #catalogSchemaPathPaginationParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "post"
		op:              "createVolume"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/volumes"
		summary:         "Create volume"
		cli:             "catalog volumes create"
		returns:         "VolumeDetail"
		success_status:  201
		error_family:    "mutating"
		params:          #catalogSchemaPathParameters
		body_ref:        "CreateVolumeRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeCreateVolumeOnCatalog
	},
	#genericOperationSpec & {
		kind:         "response"
		method:       "get"
		op:           "getVolume"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/volumes/{volume_name}"
		summary:      "Get volume"
		cli:          "catalog volumes get"
		returns:      "VolumeDetail"
		error_family: "resource"
		params:       #catalogSchemaVolumePathParameters
	},
	#genericOperationSpec & {
		kind:            "response"
		method:          "patch"
		op:              "updateVolume"
		path:            #catalogsPath + "/{catalog_name}/schemas/{schema_name}/volumes/{volume_name}"
		summary:         "Update volume"
		cli:             "catalog volumes update"
		returns:         "VolumeDetail"
		error_family:    "mutating"
		params:          #catalogSchemaVolumePathParameters
		body_ref:        "UpdateVolumeRequest"
		body_description: "Request payload"
		authz_default:   false
		authz:           #privilegeCreateVolumeOnVolume
	},
	#genericOperationSpec & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteVolume"
		path:         #catalogsPath + "/{catalog_name}/schemas/{schema_name}/volumes/{volume_name}"
		summary:      "Delete volume"
		cli:          "catalog volumes delete"
		error_family: "mutating"
		params:       #catalogSchemaVolumePathParameters
		authz_default: false
		authz:        #privilegeCreateVolumeOnVolume
	},
]

endpoints_catalogs: [
	for op in #catalogOps {
		(#endpointFromGenericOperation & {
			tag:  #catalogsTag
			spec: op
		}).endpoint
	},
]
