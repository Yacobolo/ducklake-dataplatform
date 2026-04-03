using TypeSpec.Http;

namespace Duck;

@route("/catalog-searches")
@get
@tag("Catalogs")
@summary("Search catalog")
op searchCatalog(@query query: string, @query type?: string, @query catalog?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedSearchResults> | StandardErrorResponses;

@route("/catalogs")
@post
@tag("Catalogs")
@summary("Register catalog")
op registerCatalog(@body body: CreateCatalogRequest): Created<CatalogRegistration> | MutatingErrorResponses;

@route("/catalogs")
@get
@tag("Catalogs")
@summary("List catalog registrations")
@doc("Lists registered catalogs and returns a paginated catalog registration view for management clients.")
op listCatalogs(@query max_results?: int32, @query page_token?: string): Ok<CatalogRegistrationList> | GuardedReadErrorResponses;

@route("/catalogs/{catalog_name}")
@get
@tag("Catalogs")
@summary("Get catalog")
op getCatalog(catalog_name: string): Ok<CatalogRegistration> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update catalog registration")
op updateCatalogRegistration(catalog_name: string, @body body: UpdateCatalogRegistrationRequest): Ok<CatalogRegistration> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}")
@delete
@tag("Catalogs")
@summary("Delete catalog registration")
op deleteCatalogRegistration(catalog_name: string): NoContent | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/default")
@put
@tag("Catalogs")
@summary("Set default catalog")
op setDefaultCatalog(catalog_name: string, @body body: SetDefaultCatalogRequest): Ok<CatalogRegistration> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/metastore/summary")
@get
@tag("Catalogs")
@summary("Get metastore summary")
op getMetastoreSummary(catalog_name: string): Ok<MetastoreSummary> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/version-summary")
@get
@tag("Catalogs")
@summary("Get catalog version summary")
op getCatalogVersionSummary(catalog_name: string): Ok<CatalogVersionSummary> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/history")
@get
@tag("Catalogs")
@summary("List catalog history")
op listCatalogHistory(
  catalog_name: string,
  @query entity_type?: string,
  @query schema_name?: string,
  @query table_name?: string,
  @query limit?: int32,
): Ok<CatalogHistoryResponse> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas")
@get
@tag("Catalogs")
@summary("List schemas")
op listSchemas(catalog_name: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedSchemaDetails> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas")
@post
@tag("Catalogs")
@summary("Create schema")
op createSchema(catalog_name: string, @body body: CreateSchemaRequest): Created<SchemaDetail> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}")
@get
@tag("Catalogs")
@summary("Get schema")
op getSchema(catalog_name: string, schema_name: string): Ok<SchemaDetail> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update schema")
op updateSchema(catalog_name: string, schema_name: string, @body body: UpdateSchemaRequest): Ok<SchemaDetail> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}")
@delete
@tag("Catalogs")
@summary("Delete schema")
op deleteSchema(catalog_name: string, schema_name: string, @query force?: boolean): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables")
@get
@tag("Catalogs")
@summary("List tables")
op listTables(catalog_name: string, schema_name: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedTableDetails> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables")
@post
@tag("Catalogs")
@summary("Create table")
op createTable(catalog_name: string, schema_name: string, @body body: CreateTableRequest): Created<TableDetail> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}")
@get
@tag("Catalogs")
@summary("Get table")
op getTable(catalog_name: string, schema_name: string, table_name: string): Ok<TableDetail> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update table")
op updateTable(catalog_name: string, schema_name: string, table_name: string, @body body: UpdateTableRequest): Ok<TableDetail> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}")
@delete
@tag("Catalogs")
@summary("Delete table")
op deleteTable(catalog_name: string, schema_name: string, table_name: string): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/columns")
@get
@tag("Catalogs")
@summary("List table columns")
op listTableColumns(
  catalog_name: string,
  schema_name: string,
  table_name: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedColumnDetails> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/columns/{column_name}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update column")
op updateColumn(
  catalog_name: string,
  schema_name: string,
  table_name: string,
  column_name: string,
  @body body: UpdateColumnRequest,
): Ok<ColumnDetail> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/profiles")
@post
@tag("Catalogs")
@summary("Profile table")
op profileTable(catalog_name: string, schema_name: string, table_name: string): Ok<TableStatistics> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/manifest")
@get
@tag("Catalogs")
@summary("Get table manifest")
op createManifest(catalog_name: string, schema_name: string, table_name: string): Ok<ManifestResponse> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/upload-urls")
@post
@tag("Catalogs")
@summary("Create upload URL")
op createUploadUrl(catalog_name: string, schema_name: string, table_name: string, @body body: UploadUrlRequest): Ok<UploadUrlResponse> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/ingestion-commits")
@post
@tag("Catalogs")
@summary("Commit table ingestion")
op commitTableIngestion(catalog_name: string, schema_name: string, table_name: string, @body body: CommitIngestionRequest): Ok<IngestionResult> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/ingestion-loads")
@post
@tag("Catalogs")
@summary("Load table external files")
op loadTableExternalFiles(catalog_name: string, schema_name: string, table_name: string, @body body: LoadExternalRequest): Ok<IngestionResult> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/views")
@get
@tag("Catalogs")
@summary("List views")
op listViews(catalog_name: string, schema_name: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedViewDetails> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/views")
@post
@tag("Catalogs")
@summary("Create view")
op createView(catalog_name: string, schema_name: string, @body body: CreateViewRequest): Created<ViewDetail> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/views/{view_name}")
@get
@tag("Catalogs")
@summary("Get view")
op getView(catalog_name: string, schema_name: string, view_name: string): Ok<ViewDetail> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/views/{view_name}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update view")
op updateView(catalog_name: string, schema_name: string, view_name: string, @body body: UpdateViewRequest): Ok<ViewDetail> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/views/{view_name}")
@delete
@tag("Catalogs")
@summary("Delete view")
op deleteView(catalog_name: string, schema_name: string, view_name: string): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/volumes")
@get
@tag("Catalogs")
@summary("List volumes")
op listVolumes(catalog_name: string, schema_name: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedVolumes> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/volumes")
@post
@tag("Catalogs")
@summary("Create volume")
op createVolume(catalog_name: string, schema_name: string, @body body: CreateVolumeRequest): Created<VolumeDetail> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/volumes/{volume_name}")
@get
@tag("Catalogs")
@summary("Get volume")
op getVolume(catalog_name: string, schema_name: string, volume_name: string): Ok<VolumeDetail> | ResourceErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/volumes/{volume_name}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update volume")
op updateVolume(catalog_name: string, schema_name: string, volume_name: string, @body body: UpdateVolumeRequest): Ok<VolumeDetail> | MutatingErrorResponses;

@route("/catalogs/{catalog_name}/schemas/{schema_name}/volumes/{volume_name}")
@delete
@tag("Catalogs")
@summary("Delete volume")
op deleteVolume(catalog_name: string, schema_name: string, volume_name: string): NoContent | MutatingErrorResponses;
