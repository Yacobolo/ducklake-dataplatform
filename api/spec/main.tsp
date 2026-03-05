import "@typespec/http";
import "@typespec/openapi";
import "@typespec/openapi3";

using TypeSpec.Http;
using TypeSpec.OpenAPI;

@service(#{ title: "Duck Data Platform API" })
@doc("Secure SQL query layer over DuckDB with RBAC, row-level security, and column masking, backed by SQLite metadata.")
@server("https://api.example.com", "Primary API server")
@tag("Duck")
@useAuth(BearerAuth | ApiKeyAuth<ApiKeyLocation.header, "X-API-Key">)
namespace Duck;


@error
model Error {
  code: int32;
  message: string;
  details?: Record<string>;
}

model Ok<T> {
  @statusCode statusCode: 200;
  @body body: T;
}

model Created<T> {
  @statusCode statusCode: 201;
  @body body: T;
}

model CreatedEmpty {
  @statusCode statusCode: 201;
}

model NoContent {
  @statusCode statusCode: 204;
}

@error
model UnauthorizedResponse {
  @statusCode statusCode: 401;
  @body body: Error;
}

@error
model ForbiddenResponse {
  @statusCode statusCode: 403;
  @body body: Error;
}

@error
model NotFoundResponse {
  @statusCode statusCode: 404;
  @body body: Error;
}

@error
model TooManyRequestsResponse {
  @statusCode statusCode: 429;
  @body body: Error;
}

@error
model InternalServerErrorResponse {
  @statusCode statusCode: 500;
  @body body: Error;
}

alias StandardErrorResponses = UnauthorizedResponse | TooManyRequestsResponse | InternalServerErrorResponse;
alias MutatingErrorResponses = StandardErrorResponses | ForbiddenResponse;
alias ResourceErrorResponses = MutatingErrorResponses | NotFoundResponse;

model HealthResponse {
  status: string;
}

model QueryRequest {
  sql: string;
}

model QueryResponse {
  columns: string[];
}

model Principal {
  id: string;
  name: string;
  type: string;
  is_admin: boolean;
  created_at?: string;
}

model PaginatedPrincipals {
  data: Principal[];
  next_page_token?: string;
}

model CreatePrincipalRequest {
  name: string;
  type?: string;
  is_admin?: boolean;
}

model UpdatePrincipalAdminRequest {
  is_admin: boolean;
}

model Group {
  id: string;
  name: string;
  description?: string;
  created_at?: string;
}

model PaginatedGroups {
  data: Group[];
  next_page_token?: string;
}

model CreateGroupRequest {
  name: string;
  description?: string;
}

model GroupMember {
  group_id: string;
  member_type: string;
  member_id: string;
}

model PaginatedGroupMembers {
  data: GroupMember[];
  next_page_token?: string;
}

model CreateGroupMemberRequest {
  member_type: string;
  member_id: string;
}

model PrivilegeGrant {
  id: string;
  principal_id: string;
  principal_type: string;
  securable_type: string;
  securable_id: string;
  privilege: string;
  granted_by?: string;
  granted_at?: string;
}

model PaginatedGrants {
  data: PrivilegeGrant[];
  next_page_token?: string;
}

model CreateGrantRequest {
  principal_id: string;
  principal_type: string;
  securable_type: string;
  securable_id: string;
  privilege: string;
}

model APIKeyInfo {
  id: string;
  principal_id: string;
  name: string;
  key_prefix?: string;
  expires_at?: string;
  created_at?: string;
}

model PaginatedAPIKeys {
  data: APIKeyInfo[];
  next_page_token?: string;
}

model CreateAPIKeyRequest {
  principal_id: string;
  name?: string;
  expires_at?: string;
}

model CreateAPIKeyResponse {
  id: string;
  key: string;
  name?: string;
  key_prefix?: string;
  expires_at?: string;
  created_at?: string;
}

model CleanupAPIKeysResponse {
  deleted_count: int32;
}

model RowFilter {
  id: string;
  table_id: string;
  filter_sql: string;
  description?: string;
  created_at?: string;
}

model PaginatedRowFilters {
  data: RowFilter[];
  next_page_token?: string;
}

model CreateRowFilterRequest {
  table_id?: string;
  filter_sql: string;
  description?: string;
}

model RowFilterBindingRequest {
  principal_id: string;
  principal_type: string;
}

model ColumnMask {
  id: string;
  table_id: string;
  column_name: string;
  mask_expression: string;
  description?: string;
  created_at?: string;
}

model PaginatedColumnMasks {
  data: ColumnMask[];
  next_page_token?: string;
}

model CreateColumnMaskRequest {
  column_name: string;
  mask_expression: string;
  description?: string;
}

model ColumnMaskBindingRequest {
  principal_id: string;
  principal_type: string;
  see_original?: boolean;
}

model ManifestRequest {
  table: string;
  schema?: string;
}

model ManifestColumn {
  name: string;
  type: string;
}

model ManifestResponse {
  table: string;
  schema?: string;
  columns?: ManifestColumn[];
  files?: string[];
  row_filters?: string[];
  column_masks?: Record<string>;
  expires_at?: string;
}

model AuditEntry {
  id: string;
  principal_name?: string;
  action?: string;
  statement_type?: string;
  original_sql?: string;
  rewritten_sql?: string;
  tables_accessed?: string[];
  status?: string;
  error_message?: string;
  duration_ms?: int64;
  created_at?: string;
}

model PaginatedAuditLogs {
  data: AuditEntry[];
  next_page_token?: string;
}

model QueryHistoryEntry {
  id: string;
  principal_name?: string;
  original_sql?: string;
  rewritten_sql?: string;
  statement_type?: string;
  tables_accessed?: string[];
  status?: string;
  error_message?: string;
  duration_ms?: int64;
  rows_returned?: int64;
  created_at?: string;
}

model PaginatedQueryHistoryEntries {
  data: QueryHistoryEntry[];
  next_page_token?: string;
}

model MetastoreSummary {
  catalog_name: string;
  metastore_type?: string;
  storage_backend?: string;
  data_path?: string;
  schema_count?: int32;
  table_count?: int32;
}

model CatalogRegistration {
  id: string;
  name: string;
  metastore_type?: string;
  dsn?: string;
  data_path?: string;
  status?: string;
  is_default?: boolean;
  comment?: string;
  created_at?: string;
  updated_at?: string;
}

model CatalogRegistrationList {
  catalogs: CatalogRegistration[];
  next_page_token?: string;
  total_count?: int32;
}

model CreateCatalogRequest {
  name: string;
  metastore_type?: string;
  dsn?: string;
  data_path?: string;
  comment?: string;
}

model UpdateCatalogRegistrationRequest {
  data_path?: string;
  comment?: string;
}

model SetDefaultCatalogRequest {}

model CatalogInfo {
  name: string;
  comment?: string;
  created_at?: string;
  updated_at?: string;
}

model SchemaDetail {
  schema_id: string;
  name: string;
  catalog_name: string;
  comment?: string;
  properties?: Record<string>;
  owner?: string;
  created_at?: string;
  updated_at?: string;
}

model PaginatedSchemaDetails {
  data: SchemaDetail[];
  next_page_token?: string;
}

model CreateSchemaRequest {
  name: string;
  comment?: string;
  location_name?: string;
  properties?: Record<string>;
}

model UpdateSchemaRequest {
  comment?: string;
  properties?: Record<string>;
}

model ColumnDetail {
  name: string;
  type: string;
  position?: int32;
  nullable?: boolean;
  comment?: string;
}

model PaginatedColumnDetails {
  data: ColumnDetail[];
  next_page_token?: string;
}

model CreateColumnRequest {
  name: string;
  type: string;
  nullable?: boolean;
  comment?: string;
}

model TableDetail {
  table_id: string;
  name: string;
  schema_name: string;
  catalog_name: string;
  table_type?: string;
  columns?: ColumnDetail[];
  comment?: string;
  owner?: string;
  created_at?: string;
  updated_at?: string;
}

model PaginatedTableDetails {
  data: TableDetail[];
  next_page_token?: string;
}

model CreateTableRequest {
  name: string;
  columns?: CreateColumnRequest[];
  comment?: string;
}

model UpdateTableRequest {
  comment?: string;
  owner?: string;
}

model UpdateColumnRequest {
  comment?: string;
  nullable?: boolean;
}

model TableStatistics {
  row_count?: int64;
  size_bytes?: int64;
  column_count?: int32;
  last_profiled_at?: string;
  profiled_by?: string;
}

model ViewDetail {
  id: string;
  schema_id?: string;
  schema_name: string;
  catalog_name: string;
  name: string;
  view_definition?: string;
  comment?: string;
  owner?: string;
  source_tables?: string[];
  created_at?: string;
  updated_at?: string;
}

model PaginatedViewDetails {
  data: ViewDetail[];
  next_page_token?: string;
}

model CreateViewRequest {
  name: string;
  view_definition: string;
  comment?: string;
}

model UpdateViewRequest {
  comment?: string;
  view_definition?: string;
}

model VolumeDetail {
  id: string;
  name: string;
  schema_name: string;
  catalog_name: string;
  volume_type?: string;
  storage_location?: string;
  comment?: string;
  owner?: string;
  created_at?: string;
  updated_at?: string;
}

model PaginatedVolumes {
  data: VolumeDetail[];
  next_page_token?: string;
}

model CreateVolumeRequest {
  name: string;
  volume_type?: string;
  storage_location?: string;
  comment?: string;
}

model UpdateVolumeRequest {
  comment?: string;
  new_name?: string;
  storage_location?: string;
}

@route("/healthz")
@get
op getHealth(): Ok<HealthResponse> | StandardErrorResponses;

@route("/query")
@post
op executeQuery(@body body: QueryRequest): Ok<QueryResponse> | MutatingErrorResponses;

@route("/principals")
@get
op listPrincipals(@query max_results?: int32, @query page_token?: string): Ok<PaginatedPrincipals> | StandardErrorResponses;

@route("/principals")
@post
op createPrincipal(@body body: CreatePrincipalRequest): Created<Principal> | MutatingErrorResponses;

@route("/principals/{principalId}")
@get
op getPrincipal(principalId: string): Ok<Principal> | ResourceErrorResponses;

@route("/principals/{principalId}")
@delete
op deletePrincipal(principalId: string): NoContent | MutatingErrorResponses;

@route("/principals/{principalId}/admin")
@put
op updatePrincipalAdmin(principalId: string, @body body: UpdatePrincipalAdminRequest): NoContent | MutatingErrorResponses;

@route("/groups")
@get
op listGroups(@query max_results?: int32, @query page_token?: string): Ok<PaginatedGroups> | StandardErrorResponses;

@route("/groups")
@post
op createGroup(@body body: CreateGroupRequest): Created<Group> | MutatingErrorResponses;

@route("/groups/{groupId}")
@get
op getGroup(groupId: string): Ok<Group> | ResourceErrorResponses;

@route("/groups/{groupId}")
@delete
op deleteGroup(groupId: string): NoContent | MutatingErrorResponses;

@route("/groups/{groupId}/members")
@get
op listGroupMembers(groupId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedGroupMembers> | ResourceErrorResponses;

@route("/groups/{groupId}/members")
@post
op createGroupMember(groupId: string, @body body: CreateGroupMemberRequest): CreatedEmpty | MutatingErrorResponses;

@route("/groups/{groupId}/members")
@delete
op deleteGroupMember(groupId: string, @query member_type: string, @query member_id: string): NoContent | MutatingErrorResponses;

@route("/grants")
@get
op listGrants(
  @query principal_id?: string,
  @query principal_type?: string,
  @query securable_type?: string,
  @query securable_id?: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedGrants> | StandardErrorResponses;

@route("/grants")
@post
op createGrant(@body body: CreateGrantRequest): Created<PrivilegeGrant> | MutatingErrorResponses;

@route("/grants/{grantId}")
@delete
op deleteGrant(grantId: string): NoContent | MutatingErrorResponses;

@route("/api-keys")
@get
op listAPIKeys(@query principal_id?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAPIKeys> | StandardErrorResponses;

@route("/api-keys")
@post
op createAPIKey(@body body: CreateAPIKeyRequest): Created<CreateAPIKeyResponse> | MutatingErrorResponses;

@route("/api-keys/{apiKeyId}")
@delete
op deleteAPIKey(apiKeyId: string): NoContent | MutatingErrorResponses;

@route("/api-keys/cleanup")
@post
op cleanupExpiredAPIKeys(): Ok<CleanupAPIKeysResponse> | MutatingErrorResponses;

@route("/tables/{tableId}/row-filters")
@get
op listRowFilters(tableId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedRowFilters> | ResourceErrorResponses;

@route("/tables/{tableId}/row-filters")
@post
op createRowFilter(tableId: string, @body body: CreateRowFilterRequest): Created<RowFilter> | MutatingErrorResponses;

@route("/row-filters/{rowFilterId}")
@delete
op deleteRowFilter(rowFilterId: string): NoContent | MutatingErrorResponses;

@route("/row-filters/{rowFilterId}/bindings")
@post
op bindRowFilter(rowFilterId: string, @body body: RowFilterBindingRequest): CreatedEmpty | MutatingErrorResponses;

@route("/row-filters/{rowFilterId}/bindings")
@delete
op unbindRowFilter(rowFilterId: string, @query principal_id: string, @query principal_type: string): NoContent | MutatingErrorResponses;

@route("/tables/{tableId}/column-masks")
@get
op listColumnMasks(tableId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnMasks> | ResourceErrorResponses;

@route("/tables/{tableId}/column-masks")
@post
op createColumnMask(tableId: string, @body body: CreateColumnMaskRequest): Created<ColumnMask> | MutatingErrorResponses;

@route("/column-masks/{columnMaskId}")
@delete
op deleteColumnMask(columnMaskId: string): NoContent | MutatingErrorResponses;

@route("/column-masks/{columnMaskId}/bindings")
@post
op bindColumnMask(columnMaskId: string, @body body: ColumnMaskBindingRequest): CreatedEmpty | MutatingErrorResponses;

@route("/column-masks/{columnMaskId}/bindings")
@delete
op unbindColumnMask(columnMaskId: string, @query principal_id: string, @query principal_type: string): NoContent | MutatingErrorResponses;

@route("/manifest")
@post
op createManifest(@body body: ManifestRequest): Ok<ManifestResponse> | MutatingErrorResponses;

@route("/audit-logs")
@get
op listAuditLogs(
  @query principal_name?: string,
  @query action?: string,
  @query status?: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedAuditLogs> | StandardErrorResponses;

@route("/catalogs/{catalogName}/metastore/summary")
@get
op getMetastoreSummary(catalogName: string): Ok<MetastoreSummary> | ResourceErrorResponses;

@route("/query-history")
@get
op listQueryHistory(
  @query principal_name?: string,
  @query status?: string,
  @query from?: string,
  @query to?: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedQueryHistoryEntries> | StandardErrorResponses;

@route("/catalogs")
@post
op registerCatalog(@body body: CreateCatalogRequest): Created<CatalogRegistration> | MutatingErrorResponses;

@route("/catalogs")
@get
op listCatalogs(@query max_results?: int32, @query page_token?: string): Ok<CatalogRegistrationList> | StandardErrorResponses;

@route("/catalogs/{catalogName}")
@get
op getCatalogRegistration(catalogName: string): Ok<CatalogRegistration> | ResourceErrorResponses;

@route("/catalogs/{catalogName}")
@patch(#{ implicitOptionality: true })
op updateCatalogRegistration(catalogName: string, @body body: UpdateCatalogRegistrationRequest): Ok<CatalogRegistration> | MutatingErrorResponses;

@route("/catalogs/{catalogName}")
@delete
op deleteCatalogRegistration(catalogName: string): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalogName}/set-default")
@post
op setDefaultCatalog(catalogName: string, @body body: SetDefaultCatalogRequest): Ok<CatalogRegistration> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/info")
@get
op getCatalog(catalogName: string): Ok<CatalogInfo> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas")
@get
op listSchemas(catalogName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedSchemaDetails> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas")
@post
op createSchema(catalogName: string, @body body: CreateSchemaRequest): Created<SchemaDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}")
@get
op getSchema(catalogName: string, schemaName: string): Ok<SchemaDetail> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}")
@patch(#{ implicitOptionality: true })
op updateSchema(catalogName: string, schemaName: string, @body body: UpdateSchemaRequest): Ok<SchemaDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}")
@delete
op deleteSchema(catalogName: string, schemaName: string, @query force?: boolean): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables")
@get
op listTables(catalogName: string, schemaName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedTableDetails> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables")
@post
op createTable(catalogName: string, schemaName: string, @body body: CreateTableRequest): Created<TableDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}")
@get
op getTable(catalogName: string, schemaName: string, tableName: string): Ok<TableDetail> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}")
@patch(#{ implicitOptionality: true })
op updateTable(catalogName: string, schemaName: string, tableName: string, @body body: UpdateTableRequest): Ok<TableDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}")
@delete
op deleteTable(catalogName: string, schemaName: string, tableName: string): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/columns")
@get
op listTableColumns(
  catalogName: string,
  schemaName: string,
  tableName: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedColumnDetails> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/columns/{columnName}")
@patch(#{ implicitOptionality: true })
op updateColumn(
  catalogName: string,
  schemaName: string,
  tableName: string,
  columnName: string,
  @body body: UpdateColumnRequest,
): Ok<ColumnDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/profile")
@post
op profileTable(catalogName: string, schemaName: string, tableName: string): Ok<TableStatistics> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views")
@get
op listViews(catalogName: string, schemaName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedViewDetails> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views")
@post
op createView(catalogName: string, schemaName: string, @body body: CreateViewRequest): Created<ViewDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}")
@get
op getView(catalogName: string, schemaName: string, viewName: string): Ok<ViewDetail> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}")
@patch(#{ implicitOptionality: true })
op updateView(catalogName: string, schemaName: string, viewName: string, @body body: UpdateViewRequest): Ok<ViewDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}")
@delete
op deleteView(catalogName: string, schemaName: string, viewName: string): NoContent | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes")
@get
op listVolumes(catalogName: string, schemaName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedVolumes> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes")
@post
op createVolume(catalogName: string, schemaName: string, @body body: CreateVolumeRequest): Created<VolumeDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}")
@get
op getVolume(catalogName: string, schemaName: string, volumeName: string): Ok<VolumeDetail> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}")
@patch(#{ implicitOptionality: true })
op updateVolume(catalogName: string, schemaName: string, volumeName: string, @body body: UpdateVolumeRequest): Ok<VolumeDetail> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}")
@delete
op deleteVolume(catalogName: string, schemaName: string, volumeName: string): NoContent | MutatingErrorResponses;

model GenericRequest {
  payload?: Record<string>;
}

model SubmitQueryRequest {
  sql: string;
  request_id?: string;
}

model CreateModelRequest {
  name: string;
  payload?: Record<string>;
}

model GenericResponse {
  data?: Record<string>;
}

model StorageCredential {
  id: string;
  name: string;
  credential_type?: string;
  endpoint?: string;
  region?: string;
  url_style?: string;
  comment?: string;
  owner?: string;
  created_at?: string;
  updated_at?: string;
}

model PaginatedStorageCredentials {
  data: StorageCredential[];
  next_page_token?: string;
}

model CreateStorageCredentialRequest {
  name: string;
  credential_type?: string;
  key_id?: string;
  secret?: string;
  endpoint?: string;
  region?: string;
  url_style?: string;
  comment?: string;
}

model UpdateStorageCredentialRequest {
  key_id?: string;
  secret?: string;
  endpoint?: string;
  region?: string;
  url_style?: string;
  comment?: string;
}

model ExternalLocation {
  id: string;
  name: string;
  url: string;
  credential_name?: string;
  storage_type?: string;
  comment?: string;
  owner?: string;
  read_only?: boolean;
  created_at?: string;
  updated_at?: string;
}

model PaginatedExternalLocations {
  data: ExternalLocation[];
  next_page_token?: string;
}

model CreateExternalLocationRequest {
  name: string;
  url: string;
  credential_name?: string;
  storage_type?: string;
  comment?: string;
  read_only?: boolean;
}

model UpdateExternalLocationRequest {
  url?: string;
  credential_name?: string;
  storage_type?: string;
  comment?: string;
  read_only?: boolean;
}

@route("/queries")
@post
op submitQuery(@body body: SubmitQueryRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/queries/{queryId}")
@get
op getQuery(queryId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/queries/{queryId}")
@delete
op deleteQuery(queryId: string): NoContent | MutatingErrorResponses;

@route("/queries/{queryId}/results")
@get
op getQueryResults(queryId: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/queries/{queryId}/cancel")
@post
op cancelQuery(queryId: string): Created<GenericResponse> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/upload-url")
@post
op createUploadUrl(catalogName: string, schemaName: string, tableName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/commit")
@post
op commitTableIngestion(catalogName: string, schemaName: string, tableName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/load")
@post
op loadTableExternalFiles(catalogName: string, schemaName: string, tableName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/search")
@get
op searchCatalog(@query query: string, @query type?: string, @query catalog?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}")
@get
op getTableLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/upstream")
@get
op getUpstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/downstream")
@get
op getDownstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/edges/{edgeId}")
@delete
op deleteLineageEdge(edgeId: string): NoContent | MutatingErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}")
@get
op getColumnLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}/{columnName}/impact")
@get
op getColumnImpact(schemaName: string, tableName: string, columnName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/purge")
@post
op purgeLineage(@body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/tags")
@get
op listTags(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/tags")
@post
op createTag(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/tags/{tagId}")
@delete
op deleteTag(tagId: string): NoContent | MutatingErrorResponses;

@route("/tags/{tagId}/assignments")
@post
op createTagAssignment(tagId: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/tag-assignments/{assignmentId}")
@delete
op deleteTagAssignment(assignmentId: string): NoContent | MutatingErrorResponses;

@route("/classifications")
@get
op listClassifications(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/storage-credentials")
@get
op listStorageCredentials(@query max_results?: int32, @query page_token?: string): Ok<PaginatedStorageCredentials> | StandardErrorResponses;

@route("/storage-credentials")
@post
op createStorageCredential(@body body: CreateStorageCredentialRequest): Created<StorageCredential> | MutatingErrorResponses;

@route("/storage-credentials/{credentialName}")
@get
op getStorageCredential(credentialName: string): Ok<StorageCredential> | ResourceErrorResponses;

@route("/storage-credentials/{credentialName}")
@patch(#{ implicitOptionality: true })
op updateStorageCredential(credentialName: string, @body body: UpdateStorageCredentialRequest): Ok<StorageCredential> | MutatingErrorResponses;

@route("/storage-credentials/{credentialName}")
@delete
op deleteStorageCredential(credentialName: string): NoContent | MutatingErrorResponses;

@route("/external-locations")
@get
op listExternalLocations(@query max_results?: int32, @query page_token?: string): Ok<PaginatedExternalLocations> | StandardErrorResponses;

@route("/external-locations")
@post
op createExternalLocation(@body body: CreateExternalLocationRequest): Created<ExternalLocation> | MutatingErrorResponses;

@route("/external-locations/{locationName}")
@get
op getExternalLocation(locationName: string): Ok<ExternalLocation> | ResourceErrorResponses;

@route("/external-locations/{locationName}")
@patch(#{ implicitOptionality: true })
op updateExternalLocation(locationName: string, @body body: UpdateExternalLocationRequest): Ok<ExternalLocation> | MutatingErrorResponses;

@route("/external-locations/{locationName}")
@delete
op deleteExternalLocation(locationName: string): NoContent | MutatingErrorResponses;

@route("/compute-endpoints")
@get
op listComputeEndpoints(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/compute-endpoints")
@post
op createComputeEndpoint(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}")
@get
op getComputeEndpoint(endpointName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}")
@patch(#{ implicitOptionality: true })
op updateComputeEndpoint(endpointName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}")
@delete
op deleteComputeEndpoint(endpointName: string): NoContent | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments")
@get
op listComputeAssignments(endpointName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments")
@post
op createComputeAssignment(endpointName: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}/health")
@get
op getComputeEndpointHealth(endpointName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments/{assignmentId}")
@delete
op deleteComputeAssignment(endpointName: string, assignmentId: string): NoContent | MutatingErrorResponses;

@route("/notebooks")
@get
op listNotebooks(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/notebooks")
@post
op createNotebook(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}")
@get
op getNotebook(notebookId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/notebooks/{notebookId}")
@patch(#{ implicitOptionality: true })
op updateNotebook(notebookId: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}")
@delete
op deleteNotebook(notebookId: string): NoContent | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells")
@post
op createCell(notebookId: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells/reorder")
@post
op reorderCells(notebookId: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@patch(#{ implicitOptionality: true })
op updateCell(notebookId: string, cellId: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@delete
op deleteCell(notebookId: string, cellId: string): NoContent | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions")
@post
op createNotebookSession(notebookId: string): Created<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}")
@delete
op closeNotebookSession(notebookId: string, sessionId: string): NoContent | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/execute/{cellId}")
@post
op executeCell(notebookId: string, sessionId: string, cellId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all")
@post
op runAllCells(notebookId: string, sessionId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all-async")
@post
op runAllCellsAsync(notebookId: string, sessionId: string): Created<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/jobs")
@get
op listNotebookJobs(notebookId: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/jobs/{jobId}")
@get
op getNotebookJob(notebookId: string, jobId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/git-repos")
@get
op listGitRepos(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/git-repos")
@post
op createGitRepo(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/git-repos/{gitRepoId}")
@get
op getGitRepo(gitRepoId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/git-repos/{gitRepoId}")
@delete
op deleteGitRepo(gitRepoId: string): NoContent | MutatingErrorResponses;

@route("/git-repos/{gitRepoId}/sync")
@post
op syncGitRepo(gitRepoId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/pipelines")
@get
op listPipelines(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/pipelines")
@post
op createPipeline(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}")
@get
op getPipeline(pipelineName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/pipelines/{pipelineName}")
@patch(#{ implicitOptionality: true })
op updatePipeline(pipelineName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}")
@delete
op deletePipeline(pipelineName: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/jobs")
@get
op listPipelineJobs(pipelineName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/pipelines/{pipelineName}/jobs")
@post
op createPipelineJob(pipelineName: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/jobs/{jobId}")
@delete
op deletePipelineJob(pipelineName: string, jobId: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/runs")
@post
op triggerPipelineRun(pipelineName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/runs")
@get
op listPipelineRuns(pipelineName: string, @query max_results?: int32, @query page_token?: string, @query status?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/pipelines/runs/{runId}")
@get
op getPipelineRun(runId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/pipelines/runs/{runId}/cancel")
@post
op cancelPipelineRun(runId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/runs/{runId}/jobs")
@get
op listPipelineJobRuns(runId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models")
@get
op listModels(@query project_name?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/models")
@post
op createModel(@body body: CreateModelRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}")
@get
op getModel(projectName: string, modelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}")
@patch(#{ implicitOptionality: true })
op updateModel(projectName: string, modelName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}")
@delete
op deleteModel(projectName: string, modelName: string): NoContent | MutatingErrorResponses;

@route("/models/dag")
@get
op getModelDAG(@query project_name?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/model-runs")
@post
op triggerModelRun(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/model-runs")
@get
op listModelRuns(@query status?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/model-runs/{runId}")
@get
op getModelRun(runId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/model-runs/{runId}/steps")
@get
op listModelRunSteps(runId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@post
op createModelTest(projectName: string, modelName: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@get
op listModelTests(projectName: string, modelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests/{testId}")
@delete
op deleteModelTest(projectName: string, modelName: string, testId: string): NoContent | MutatingErrorResponses;

@route("/model-runs/{runId}/steps/{stepId}/test-results")
@get
op listModelTestResults(runId: string, stepId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/freshness")
@get
op checkModelFreshness(projectName: string, modelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/sources/{sourceSchema}/{sourceTable}/freshness")
@get
op checkSourceFreshness(sourceSchema: string, sourceTable: string, @query timestamp_column?: string, @query max_lag_seconds?: int64): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/from-notebook")
@post
op promoteNotebookToModel(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/model-runs/{runId}/cancel")
@post
op cancelModelRun(runId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/macros")
@get
op listMacros(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/macros")
@post
op createMacro(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/macros/{macroName}")
@get
op getMacro(macroName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/macros/{macroName}")
@patch(#{ implicitOptionality: true })
op updateMacro(macroName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/macros/{macroName}")
@delete
op deleteMacro(macroName: string): NoContent | MutatingErrorResponses;

@route("/macros/{macroName}/revisions")
@get
op listMacroRevisions(macroName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/macros/{macroName}/impact")
@get
op getMacroImpact(macroName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/macros/{macroName}/diff")
@get
op diffMacroRevisions(macroName: string, @query from_version: int32, @query to_version: int32): Ok<GenericResponse> | ResourceErrorResponses;

@route("/semantic-models")
@get
op listSemanticModels(@query project_name?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/semantic-models")
@post
op createSemanticModel(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@get
op getSemanticModel(projectName: string, semanticModelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@patch(#{ implicitOptionality: true })
op updateSemanticModel(projectName: string, semanticModelName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@delete
op deleteSemanticModel(projectName: string, semanticModelName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@get
op listSemanticMetrics(projectName: string, semanticModelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@post
op createSemanticMetric(projectName: string, semanticModelName: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@patch(#{ implicitOptionality: true })
op updateSemanticMetric(projectName: string, semanticModelName: string, metricName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@delete
op deleteSemanticMetric(projectName: string, semanticModelName: string, metricName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@get
op listSemanticPreAggregations(projectName: string, semanticModelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@post
op createSemanticPreAggregation(projectName: string, semanticModelName: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@patch(#{ implicitOptionality: true })
op updateSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@delete
op deleteSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string): NoContent | MutatingErrorResponses;

@route("/semantic-relationships")
@get
op listSemanticRelationships(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/semantic-relationships")
@post
op createSemanticRelationship(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/semantic-relationships/{relationshipName}")
@patch(#{ implicitOptionality: true })
op updateSemanticRelationship(relationshipName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/semantic-relationships/{relationshipName}")
@delete
op deleteSemanticRelationship(relationshipName: string): NoContent | MutatingErrorResponses;

@route("/metric-queries:explain")
@post
op explainMetricQuery(@body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/metric-queries:run")
@post
op runMetricQuery(@body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/metrics/{metricName}/freshness")
@get
op checkMetricFreshness(metricName: string, @query project_name?: string, @query semantic_model_name?: string): Ok<GenericResponse> | ResourceErrorResponses;

@@extension(createSchema, "x-authz", #{ mode: "authenticated" });
@@extension(updateSchema, "x-authz", #{ mode: "authenticated" });
@@extension(deleteSchema, "x-authz", #{ mode: "authenticated" });
@@extension(createTable, "x-authz", #{ mode: "authenticated" });
@@extension(updateTable, "x-authz", #{ mode: "authenticated" });
@@extension(deleteTable, "x-authz", #{ mode: "authenticated" });
@@extension(updateColumn, "x-authz", #{ mode: "authenticated" });
@@extension(createView, "x-authz", #{ mode: "authenticated" });
@@extension(updateView, "x-authz", #{ mode: "authenticated" });
@@extension(deleteView, "x-authz", #{ mode: "authenticated" });
@@extension(createVolume, "x-authz", #{ mode: "authenticated" });
@@extension(updateVolume, "x-authz", #{ mode: "authenticated" });
@@extension(deleteVolume, "x-authz", #{ mode: "authenticated" });
@@extension(createComputeEndpoint, "x-authz", #{ mode: "authenticated" });
@@extension(updateComputeEndpoint, "x-authz", #{ mode: "authenticated" });
@@extension(deleteComputeEndpoint, "x-authz", #{ mode: "authenticated" });
@@extension(createComputeAssignment, "x-authz", #{ mode: "authenticated" });
@@extension(deleteComputeAssignment, "x-authz", #{ mode: "authenticated" });
@@extension(createExternalLocation, "x-authz", #{ mode: "authenticated" });
@@extension(updateExternalLocation, "x-authz", #{ mode: "authenticated" });
@@extension(deleteExternalLocation, "x-authz", #{ mode: "authenticated" });
@@extension(createGrant, "x-authz", #{ mode: "authenticated" });
@@extension(deleteGrant, "x-authz", #{ mode: "authenticated" });
@@extension(createStorageCredential, "x-authz", #{ mode: "authenticated" });
@@extension(updateStorageCredential, "x-authz", #{ mode: "authenticated" });
@@extension(deleteStorageCredential, "x-authz", #{ mode: "authenticated" });
