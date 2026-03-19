using TypeSpec.Http;

namespace Duck;

@route("/catalogs:search")
@get
@tag("Catalogs")
@summary("Search catalog")
op searchCatalog(@query query: string, @query type?: string, @query catalog?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedSearchResults> | StandardErrorResponses;

@route("/catalog-registrations")
@post
@tag("Catalogs")
@summary("Register catalog")
op registerCatalog(@body body: CreateCatalogRequest): Created<CatalogRegistration> | MutatingErrorResponses;

@route("/catalog-registrations")
@get
@tag("Catalogs")
@summary("List catalog registrations")
@doc("Lists registered catalogs and returns a paginated catalog registration view for management clients.")
op listCatalogs(@query max_results?: int32, @query page_token?: string): Ok<CatalogRegistrationList> | GuardedReadErrorResponses;

@route("/catalog-registrations/{catalogName}")
@get
@tag("Catalogs")
@summary("Get catalog registration")
op getCatalogRegistration(catalogName: string): Ok<CatalogRegistration> | ResourceErrorResponses;

@route("/catalog-registrations/{catalogName}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update catalog registration")
op updateCatalogRegistration(catalogName: string, @body body: UpdateCatalogRegistrationRequest): Ok<CatalogRegistration> | ResourceErrorResponses;

@route("/catalog-registrations/{catalogName}")
@delete
@tag("Catalogs")
@summary("Delete catalog registration")
op deleteCatalogRegistration(catalogName: string): NoContent | ResourceErrorResponses;

@route("/catalog-registrations/{catalogName}:set-default")
@post
@tag("Catalogs")
@summary("Set default catalog")
op setDefaultCatalog(catalogName: string, @body body: SetDefaultCatalogRequest): Ok<CatalogRegistration> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/info")
@get
@tag("Catalogs")
@summary("Get catalog")
@doc("Returns runtime catalog information, including metastore details and status for the named catalog.")
op getCatalog(catalogName: string): Ok<CatalogInfo> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/metastore/summary")
@get
@tag("Catalogs")
@summary("Get metastore summary")
op getMetastoreSummary(catalogName: string): Ok<MetastoreSummary> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/version-summary")
@get
@tag("Catalogs")
@summary("Get catalog version summary")
op getCatalogVersionSummary(catalogName: string): Ok<CatalogVersionSummary> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/history")
@get
@tag("Catalogs")
@summary("List catalog history")
op listCatalogHistory(
  catalogName: string,
  @query entity_type?: string,
  @query schema_name?: string,
  @query table_name?: string,
  @query limit?: int32,
): Ok<CatalogHistoryResponse> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas")
@get
@tag("Catalogs")
@summary("List schemas")
op listSchemas(catalogName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedSchemaDetails> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas")
@post
@tag("Catalogs")
@summary("Create schema")
op createSchema(catalogName: string, @body body: CreateSchemaRequest): Created<SchemaDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}")
@get
@tag("Catalogs")
@summary("Get schema")
op getSchema(catalogName: string, schemaName: string): Ok<SchemaDetail> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update schema")
op updateSchema(catalogName: string, schemaName: string, @body body: UpdateSchemaRequest): Ok<SchemaDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}")
@delete
@tag("Catalogs")
@summary("Delete schema")
op deleteSchema(catalogName: string, schemaName: string, @query force?: boolean): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables")
@get
@tag("Catalogs")
@summary("List tables")
op listTables(catalogName: string, schemaName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedTableDetails> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables")
@post
@tag("Catalogs")
@summary("Create table")
op createTable(catalogName: string, schemaName: string, @body body: CreateTableRequest): Created<TableDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}")
@get
@tag("Catalogs")
@summary("Get table")
op getTable(catalogName: string, schemaName: string, tableName: string): Ok<TableDetail> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update table")
op updateTable(catalogName: string, schemaName: string, tableName: string, @body body: UpdateTableRequest): Ok<TableDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}")
@delete
@tag("Catalogs")
@summary("Delete table")
op deleteTable(catalogName: string, schemaName: string, tableName: string): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/columns")
@get
@tag("Catalogs")
@summary("List table columns")
op listTableColumns(
  catalogName: string,
  schemaName: string,
  tableName: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedColumnDetails> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/columns/{columnName}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update column")
op updateColumn(
  catalogName: string,
  schemaName: string,
  tableName: string,
  columnName: string,
  @body body: UpdateColumnRequest,
): Ok<ColumnDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/profile")
@post
@tag("Catalogs")
@summary("Profile table")
op profileTable(catalogName: string, schemaName: string, tableName: string): Ok<TableStatistics> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/manifest")
@get
@tag("Catalogs")
@summary("Get table manifest")
op createManifest(catalogName: string, schemaName: string, tableName: string): Ok<ManifestResponse> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/upload-url")
@post
@tag("Catalogs")
@summary("Create upload URL")
op createUploadUrl(catalogName: string, schemaName: string, tableName: string, @body body: UploadUrlRequest): Ok<UploadUrlResponse> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/commit")
@post
@tag("Catalogs")
@summary("Commit table ingestion")
op commitTableIngestion(catalogName: string, schemaName: string, tableName: string, @body body: CommitIngestionRequest): Ok<IngestionResult> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/load")
@post
@tag("Catalogs")
@summary("Load table external files")
op loadTableExternalFiles(catalogName: string, schemaName: string, tableName: string, @body body: LoadExternalRequest): Ok<IngestionResult> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views")
@get
@tag("Catalogs")
@summary("List views")
op listViews(catalogName: string, schemaName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedViewDetails> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views")
@post
@tag("Catalogs")
@summary("Create view")
op createView(catalogName: string, schemaName: string, @body body: CreateViewRequest): Created<ViewDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}")
@get
@tag("Catalogs")
@summary("Get view")
op getView(catalogName: string, schemaName: string, viewName: string): Ok<ViewDetail> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update view")
op updateView(catalogName: string, schemaName: string, viewName: string, @body body: UpdateViewRequest): Ok<ViewDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}")
@delete
@tag("Catalogs")
@summary("Delete view")
op deleteView(catalogName: string, schemaName: string, viewName: string): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes")
@get
@tag("Catalogs")
@summary("List volumes")
op listVolumes(catalogName: string, schemaName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedVolumes> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes")
@post
@tag("Catalogs")
@summary("Create volume")
op createVolume(catalogName: string, schemaName: string, @body body: CreateVolumeRequest): Created<VolumeDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}")
@get
@tag("Catalogs")
@summary("Get volume")
op getVolume(catalogName: string, schemaName: string, volumeName: string): Ok<VolumeDetail> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update volume")
op updateVolume(catalogName: string, schemaName: string, volumeName: string, @body body: UpdateVolumeRequest): Ok<VolumeDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}")
@delete
@tag("Catalogs")
@summary("Delete volume")
op deleteVolume(catalogName: string, schemaName: string, volumeName: string): NoContent | MutatingErrorResponses;
