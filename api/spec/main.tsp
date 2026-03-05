import "@typespec/http";
import "@typespec/openapi3";

using TypeSpec.Http;

@service(#{ title: "Duck Data Platform API" })
namespace Duck;

model HealthResponse {
  status: string;
}

model QueryRequest {
  sql: string;
}

model QueryResponse {
  columns: string[];
}

model Error {
  code: int32;
  message: string;
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
op getHealth(): HealthResponse;

@route("/query")
@post
op executeQuery(@body body: QueryRequest): QueryResponse;

@route("/principals")
@get
op listPrincipals(max_results?: int32, page_token?: string): PaginatedPrincipals;

@route("/principals")
@post
op createPrincipal(@body body: CreatePrincipalRequest): Principal;

@route("/principals/{principalId}")
@get
op getPrincipal(principalId: string): Principal;

@route("/principals/{principalId}")
@delete
op deletePrincipal(principalId: string): void;

@route("/principals/{principalId}/admin")
@put
op updatePrincipalAdmin(principalId: string, @body body: UpdatePrincipalAdminRequest): void;

@route("/groups")
@get
op listGroups(max_results?: int32, page_token?: string): PaginatedGroups;

@route("/groups")
@post
op createGroup(@body body: CreateGroupRequest): Group;

@route("/groups/{groupId}")
@get
op getGroup(groupId: string): Group;

@route("/groups/{groupId}")
@delete
op deleteGroup(groupId: string): void;

@route("/groups/{groupId}/members")
@get
op listGroupMembers(groupId: string, max_results?: int32, page_token?: string): PaginatedGroupMembers;

@route("/groups/{groupId}/members")
@post
op createGroupMember(groupId: string, @body body: CreateGroupMemberRequest): void;

@route("/groups/{groupId}/members")
@delete
op deleteGroupMember(groupId: string, member_type: string, member_id: string): void;

@route("/grants")
@get
op listGrants(
  principal_id?: string,
  principal_type?: string,
  securable_type?: string,
  securable_id?: string,
  max_results?: int32,
  page_token?: string,
): PaginatedGrants;

@route("/grants")
@post
op createGrant(@body body: CreateGrantRequest): PrivilegeGrant;

@route("/grants/{grantId}")
@delete
op deleteGrant(grantId: string): void;

@route("/api-keys")
@get
op listAPIKeys(principal_id?: string, max_results?: int32, page_token?: string): PaginatedAPIKeys;

@route("/api-keys")
@post
op createAPIKey(@body body: CreateAPIKeyRequest): CreateAPIKeyResponse;

@route("/api-keys/{apiKeyId}")
@delete
op deleteAPIKey(apiKeyId: string): void;

@route("/api-keys/cleanup")
@post
op cleanupExpiredAPIKeys(): CleanupAPIKeysResponse;

@route("/tables/{tableId}/row-filters")
@get
op listRowFilters(tableId: string, max_results?: int32, page_token?: string): PaginatedRowFilters;

@route("/tables/{tableId}/row-filters")
@post
op createRowFilter(tableId: string, @body body: CreateRowFilterRequest): RowFilter;

@route("/row-filters/{rowFilterId}")
@delete
op deleteRowFilter(rowFilterId: string): void;

@route("/row-filters/{rowFilterId}/bindings")
@post
op bindRowFilter(rowFilterId: string, @body body: RowFilterBindingRequest): void;

@route("/row-filters/{rowFilterId}/bindings")
@delete
op unbindRowFilter(rowFilterId: string, principal_id: string, principal_type: string): void;

@route("/tables/{tableId}/column-masks")
@get
op listColumnMasks(tableId: string, max_results?: int32, page_token?: string): PaginatedColumnMasks;

@route("/tables/{tableId}/column-masks")
@post
op createColumnMask(tableId: string, @body body: CreateColumnMaskRequest): ColumnMask;

@route("/column-masks/{columnMaskId}")
@delete
op deleteColumnMask(columnMaskId: string): void;

@route("/column-masks/{columnMaskId}/bindings")
@post
op bindColumnMask(columnMaskId: string, @body body: ColumnMaskBindingRequest): void;

@route("/column-masks/{columnMaskId}/bindings")
@delete
op unbindColumnMask(columnMaskId: string, principal_id: string, principal_type: string): void;

@route("/manifest")
@post
op createManifest(@body body: ManifestRequest): ManifestResponse;

@route("/audit-logs")
@get
op listAuditLogs(
  principal_name?: string,
  action?: string,
  status?: string,
  max_results?: int32,
  page_token?: string,
): PaginatedAuditLogs;

@route("/catalogs/{catalogName}/metastore/summary")
@get
op getMetastoreSummary(catalogName: string): MetastoreSummary;

@route("/query-history")
@get
op listQueryHistory(
  principal_name?: string,
  status?: string,
  from?: string,
  to?: string,
  max_results?: int32,
  page_token?: string,
): PaginatedQueryHistoryEntries;

@route("/catalogs")
@post
op registerCatalog(@body body: CreateCatalogRequest): CatalogRegistration;

@route("/catalogs")
@get
op listCatalogs(max_results?: int32, page_token?: string): CatalogRegistrationList;

@route("/catalogs/{catalogName}")
@get
op getCatalogRegistration(catalogName: string): CatalogRegistration;

@route("/catalogs/{catalogName}")
@patch(#{ implicitOptionality: true })
op updateCatalogRegistration(catalogName: string, @body body: UpdateCatalogRegistrationRequest): CatalogRegistration;

@route("/catalogs/{catalogName}")
@delete
op deleteCatalogRegistration(catalogName: string): void;

@route("/catalogs/{catalogName}/set-default")
@post
op setDefaultCatalog(catalogName: string, @body body: SetDefaultCatalogRequest): CatalogRegistration;

@route("/catalogs/{catalogName}/info")
@get
op getCatalog(catalogName: string): CatalogInfo;

@route("/catalogs/{catalogName}/schemas")
@get
op listSchemas(catalogName: string, max_results?: int32, page_token?: string): PaginatedSchemaDetails;

@route("/catalogs/{catalogName}/schemas")
@post
op createSchema(catalogName: string, @body body: CreateSchemaRequest): SchemaDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}")
@get
op getSchema(catalogName: string, schemaName: string): SchemaDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}")
@patch(#{ implicitOptionality: true })
op updateSchema(catalogName: string, schemaName: string, @body body: UpdateSchemaRequest): SchemaDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}")
@delete
op deleteSchema(catalogName: string, schemaName: string, force?: boolean): void;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables")
@get
op listTables(catalogName: string, schemaName: string, max_results?: int32, page_token?: string): PaginatedTableDetails;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables")
@post
op createTable(catalogName: string, schemaName: string, @body body: CreateTableRequest): TableDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}")
@get
op getTable(catalogName: string, schemaName: string, tableName: string): TableDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}")
@patch(#{ implicitOptionality: true })
op updateTable(catalogName: string, schemaName: string, tableName: string, @body body: UpdateTableRequest): TableDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}")
@delete
op deleteTable(catalogName: string, schemaName: string, tableName: string): void;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/columns")
@get
op listTableColumns(
  catalogName: string,
  schemaName: string,
  tableName: string,
  max_results?: int32,
  page_token?: string,
): PaginatedColumnDetails;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/columns/{columnName}")
@patch(#{ implicitOptionality: true })
op updateColumn(
  catalogName: string,
  schemaName: string,
  tableName: string,
  columnName: string,
  @body body: UpdateColumnRequest,
): ColumnDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/profile")
@post
op profileTable(catalogName: string, schemaName: string, tableName: string): TableStatistics;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views")
@get
op listViews(catalogName: string, schemaName: string, max_results?: int32, page_token?: string): PaginatedViewDetails;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views")
@post
op createView(catalogName: string, schemaName: string, @body body: CreateViewRequest): ViewDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}")
@get
op getView(catalogName: string, schemaName: string, viewName: string): ViewDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}")
@patch(#{ implicitOptionality: true })
op updateView(catalogName: string, schemaName: string, viewName: string, @body body: UpdateViewRequest): ViewDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}")
@delete
op deleteView(catalogName: string, schemaName: string, viewName: string): void;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes")
@get
op listVolumes(catalogName: string, schemaName: string, max_results?: int32, page_token?: string): PaginatedVolumes;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes")
@post
op createVolume(catalogName: string, schemaName: string, @body body: CreateVolumeRequest): VolumeDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}")
@get
op getVolume(catalogName: string, schemaName: string, volumeName: string): VolumeDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}")
@patch(#{ implicitOptionality: true })
op updateVolume(catalogName: string, schemaName: string, volumeName: string, @body body: UpdateVolumeRequest): VolumeDetail;

@route("/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}")
@delete
op deleteVolume(catalogName: string, schemaName: string, volumeName: string): void;

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
op submitQuery(@body body: SubmitQueryRequest): GenericResponse;

@route("/queries/{queryId}")
@get
op getQuery(queryId: string): GenericResponse;

@route("/queries/{queryId}")
@delete
op deleteQuery(queryId: string): void;

@route("/queries/{queryId}/results")
@get
op getQueryResults(queryId: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/queries/{queryId}/cancel")
@post
op cancelQuery(queryId: string): GenericResponse;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/upload-url")
@post
op createUploadUrl(catalogName: string, schemaName: string, tableName: string, @body body: GenericRequest): GenericResponse;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/commit")
@post
op commitTableIngestion(catalogName: string, schemaName: string, tableName: string, @body body: GenericRequest): GenericResponse;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/load")
@post
op loadTableExternalFiles(catalogName: string, schemaName: string, tableName: string, @body body: GenericRequest): GenericResponse;

@route("/search")
@get
op searchCatalog(query: string, type?: string, catalog?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/tables/{schemaName}/{tableName}")
@get
op getTableLineage(schemaName: string, tableName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/tables/{schemaName}/{tableName}/upstream")
@get
op getUpstreamLineage(schemaName: string, tableName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/tables/{schemaName}/{tableName}/downstream")
@get
op getDownstreamLineage(schemaName: string, tableName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/edges/{edgeId}")
@delete
op deleteLineageEdge(edgeId: string): void;

@route("/lineage/columns/{schemaName}/{tableName}")
@get
op getColumnLineage(schemaName: string, tableName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/columns/{schemaName}/{tableName}/{columnName}/impact")
@get
op getColumnImpact(schemaName: string, tableName: string, columnName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/purge")
@post
op purgeLineage(@body body: GenericRequest): GenericResponse;

@route("/tags")
@get
op listTags(max_results?: int32, page_token?: string): GenericResponse;

@route("/tags")
@post
op createTag(@body body: GenericRequest): GenericResponse;

@route("/tags/{tagId}")
@delete
op deleteTag(tagId: string): void;

@route("/tags/{tagId}/assignments")
@post
op createTagAssignment(tagId: string, @body body: GenericRequest): GenericResponse;

@route("/tag-assignments/{assignmentId}")
@delete
op deleteTagAssignment(assignmentId: string): void;

@route("/classifications")
@get
op listClassifications(max_results?: int32, page_token?: string): GenericResponse;

@route("/storage-credentials")
@get
op listStorageCredentials(max_results?: int32, page_token?: string): PaginatedStorageCredentials;

@route("/storage-credentials")
@post
op createStorageCredential(@body body: CreateStorageCredentialRequest): StorageCredential;

@route("/storage-credentials/{credentialName}")
@get
op getStorageCredential(credentialName: string): StorageCredential;

@route("/storage-credentials/{credentialName}")
@patch(#{ implicitOptionality: true })
op updateStorageCredential(credentialName: string, @body body: UpdateStorageCredentialRequest): StorageCredential;

@route("/storage-credentials/{credentialName}")
@delete
op deleteStorageCredential(credentialName: string): void;

@route("/external-locations")
@get
op listExternalLocations(max_results?: int32, page_token?: string): PaginatedExternalLocations;

@route("/external-locations")
@post
op createExternalLocation(@body body: CreateExternalLocationRequest): ExternalLocation;

@route("/external-locations/{locationName}")
@get
op getExternalLocation(locationName: string): ExternalLocation;

@route("/external-locations/{locationName}")
@patch(#{ implicitOptionality: true })
op updateExternalLocation(locationName: string, @body body: UpdateExternalLocationRequest): ExternalLocation;

@route("/external-locations/{locationName}")
@delete
op deleteExternalLocation(locationName: string): void;

@route("/compute-endpoints")
@get
op listComputeEndpoints(max_results?: int32, page_token?: string): GenericResponse;

@route("/compute-endpoints")
@post
op createComputeEndpoint(@body body: GenericRequest): GenericResponse;

@route("/compute-endpoints/{endpointName}")
@get
op getComputeEndpoint(endpointName: string): GenericResponse;

@route("/compute-endpoints/{endpointName}")
@patch(#{ implicitOptionality: true })
op updateComputeEndpoint(endpointName: string, @body body: GenericRequest): GenericResponse;

@route("/compute-endpoints/{endpointName}")
@delete
op deleteComputeEndpoint(endpointName: string): void;

@route("/compute-endpoints/{endpointName}/assignments")
@get
op listComputeAssignments(endpointName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/compute-endpoints/{endpointName}/assignments")
@post
op createComputeAssignment(endpointName: string, @body body: GenericRequest): GenericResponse;

@route("/compute-endpoints/{endpointName}/health")
@get
op getComputeEndpointHealth(endpointName: string): GenericResponse;

@route("/compute-endpoints/{endpointName}/assignments/{assignmentId}")
@delete
op deleteComputeAssignment(endpointName: string, assignmentId: string): void;

@route("/notebooks")
@get
op listNotebooks(owner?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/notebooks")
@post
op createNotebook(@body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}")
@get
op getNotebook(notebookId: string): GenericResponse;

@route("/notebooks/{notebookId}")
@patch(#{ implicitOptionality: true })
op updateNotebook(notebookId: string, @body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}")
@delete
op deleteNotebook(notebookId: string): void;

@route("/notebooks/{notebookId}/cells")
@post
op createCell(notebookId: string, @body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}/cells/reorder")
@post
op reorderCells(notebookId: string, @body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}/cells/{cellId}")
@patch(#{ implicitOptionality: true })
op updateCell(notebookId: string, cellId: string, @body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}/cells/{cellId}")
@delete
op deleteCell(notebookId: string, cellId: string): void;

@route("/notebooks/{notebookId}/sessions")
@post
op createNotebookSession(notebookId: string): GenericResponse;

@route("/notebooks/{notebookId}/sessions/{sessionId}")
@delete
op closeNotebookSession(notebookId: string, sessionId: string): void;

@route("/notebooks/{notebookId}/sessions/{sessionId}/execute/{cellId}")
@post
op executeCell(notebookId: string, sessionId: string, cellId: string): GenericResponse;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all")
@post
op runAllCells(notebookId: string, sessionId: string): GenericResponse;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all-async")
@post
op runAllCellsAsync(notebookId: string, sessionId: string): GenericResponse;

@route("/notebooks/{notebookId}/jobs")
@get
op listNotebookJobs(notebookId: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/notebooks/{notebookId}/jobs/{jobId}")
@get
op getNotebookJob(notebookId: string, jobId: string): GenericResponse;

@route("/git-repos")
@get
op listGitRepos(max_results?: int32, page_token?: string): GenericResponse;

@route("/git-repos")
@post
op createGitRepo(@body body: GenericRequest): GenericResponse;

@route("/git-repos/{gitRepoId}")
@get
op getGitRepo(gitRepoId: string): GenericResponse;

@route("/git-repos/{gitRepoId}")
@delete
op deleteGitRepo(gitRepoId: string): void;

@route("/git-repos/{gitRepoId}/sync")
@post
op syncGitRepo(gitRepoId: string): GenericResponse;

@route("/pipelines")
@get
op listPipelines(max_results?: int32, page_token?: string): GenericResponse;

@route("/pipelines")
@post
op createPipeline(@body body: GenericRequest): GenericResponse;

@route("/pipelines/{pipelineName}")
@get
op getPipeline(pipelineName: string): GenericResponse;

@route("/pipelines/{pipelineName}")
@patch(#{ implicitOptionality: true })
op updatePipeline(pipelineName: string, @body body: GenericRequest): GenericResponse;

@route("/pipelines/{pipelineName}")
@delete
op deletePipeline(pipelineName: string): void;

@route("/pipelines/{pipelineName}/jobs")
@get
op listPipelineJobs(pipelineName: string): GenericResponse;

@route("/pipelines/{pipelineName}/jobs")
@post
op createPipelineJob(pipelineName: string, @body body: GenericRequest): GenericResponse;

@route("/pipelines/{pipelineName}/jobs/{jobId}")
@delete
op deletePipelineJob(pipelineName: string, jobId: string): void;

@route("/pipelines/{pipelineName}/runs")
@post
op triggerPipelineRun(pipelineName: string, @body body: GenericRequest): GenericResponse;

@route("/pipelines/{pipelineName}/runs")
@get
op listPipelineRuns(pipelineName: string, max_results?: int32, page_token?: string, status?: string): GenericResponse;

@route("/pipelines/runs/{runId}")
@get
op getPipelineRun(runId: string): GenericResponse;

@route("/pipelines/runs/{runId}/cancel")
@post
op cancelPipelineRun(runId: string): GenericResponse;

@route("/pipelines/runs/{runId}/jobs")
@get
op listPipelineJobRuns(runId: string): GenericResponse;

@route("/models")
@get
op listModels(project_name?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/models")
@post
op createModel(@body body: CreateModelRequest): GenericResponse;

@route("/models/{projectName}/{modelName}")
@get
op getModel(projectName: string, modelName: string): GenericResponse;

@route("/models/{projectName}/{modelName}")
@patch(#{ implicitOptionality: true })
op updateModel(projectName: string, modelName: string, @body body: GenericRequest): GenericResponse;

@route("/models/{projectName}/{modelName}")
@delete
op deleteModel(projectName: string, modelName: string): void;

@route("/models/dag")
@get
op getModelDAG(project_name?: string): GenericResponse;

@route("/model-runs")
@post
op triggerModelRun(@body body: GenericRequest): GenericResponse;

@route("/model-runs")
@get
op listModelRuns(status?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/model-runs/{runId}")
@get
op getModelRun(runId: string): GenericResponse;

@route("/model-runs/{runId}/steps")
@get
op listModelRunSteps(runId: string): GenericResponse;

@route("/models/{projectName}/{modelName}/tests")
@post
op createModelTest(projectName: string, modelName: string, @body body: GenericRequest): GenericResponse;

@route("/models/{projectName}/{modelName}/tests")
@get
op listModelTests(projectName: string, modelName: string): GenericResponse;

@route("/models/{projectName}/{modelName}/tests/{testId}")
@delete
op deleteModelTest(projectName: string, modelName: string, testId: string): void;

@route("/model-runs/{runId}/steps/{stepId}/test-results")
@get
op listModelTestResults(runId: string, stepId: string): GenericResponse;

@route("/models/{projectName}/{modelName}/freshness")
@get
op checkModelFreshness(projectName: string, modelName: string): GenericResponse;

@route("/sources/{sourceSchema}/{sourceTable}/freshness")
@get
op checkSourceFreshness(sourceSchema: string, sourceTable: string, timestamp_column?: string, max_lag_seconds?: int64): GenericResponse;

@route("/models/from-notebook")
@post
op promoteNotebookToModel(@body body: GenericRequest): GenericResponse;

@route("/model-runs/{runId}/cancel")
@post
op cancelModelRun(runId: string): GenericResponse;

@route("/macros")
@get
op listMacros(max_results?: int32, page_token?: string): GenericResponse;

@route("/macros")
@post
op createMacro(@body body: GenericRequest): GenericResponse;

@route("/macros/{macroName}")
@get
op getMacro(macroName: string): GenericResponse;

@route("/macros/{macroName}")
@patch(#{ implicitOptionality: true })
op updateMacro(macroName: string, @body body: GenericRequest): GenericResponse;

@route("/macros/{macroName}")
@delete
op deleteMacro(macroName: string): void;

@route("/macros/{macroName}/revisions")
@get
op listMacroRevisions(macroName: string): GenericResponse;

@route("/macros/{macroName}/impact")
@get
op getMacroImpact(macroName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/macros/{macroName}/diff")
@get
op diffMacroRevisions(macroName: string, from_version: int32, to_version: int32): GenericResponse;

@route("/semantic-models")
@get
op listSemanticModels(project_name?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/semantic-models")
@post
op createSemanticModel(@body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}")
@get
op getSemanticModel(projectName: string, semanticModelName: string): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}")
@patch(#{ implicitOptionality: true })
op updateSemanticModel(projectName: string, semanticModelName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}")
@delete
op deleteSemanticModel(projectName: string, semanticModelName: string): void;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@get
op listSemanticMetrics(projectName: string, semanticModelName: string): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@post
op createSemanticMetric(projectName: string, semanticModelName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@patch(#{ implicitOptionality: true })
op updateSemanticMetric(projectName: string, semanticModelName: string, metricName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@delete
op deleteSemanticMetric(projectName: string, semanticModelName: string, metricName: string): void;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@get
op listSemanticPreAggregations(projectName: string, semanticModelName: string): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@post
op createSemanticPreAggregation(projectName: string, semanticModelName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@patch(#{ implicitOptionality: true })
op updateSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@delete
op deleteSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string): void;

@route("/semantic-relationships")
@get
op listSemanticRelationships(max_results?: int32, page_token?: string): GenericResponse;

@route("/semantic-relationships")
@post
op createSemanticRelationship(@body body: GenericRequest): GenericResponse;

@route("/semantic-relationships/{relationshipName}")
@patch(#{ implicitOptionality: true })
op updateSemanticRelationship(relationshipName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-relationships/{relationshipName}")
@delete
op deleteSemanticRelationship(relationshipName: string): void;

@route("/metric-queries:explain")
@post
op explainMetricQuery(@body body: GenericRequest): GenericResponse;

@route("/metric-queries:run")
@post
op runMetricQuery(@body body: GenericRequest): GenericResponse;

@route("/metrics/{metricName}/freshness")
@get
op checkMetricFreshness(metricName: string, project_name?: string, semantic_model_name?: string): GenericResponse;
