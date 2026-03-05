using TypeSpec.Http;

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
