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

model Accepted<T> {
  @statusCode statusCode: 202;
  @body body: T;
}

model CreatedEmpty {
  @statusCode statusCode: 201;
}

model NoContent {
  @statusCode statusCode: 204;
}

@error
model BadRequestResponse {
  @statusCode statusCode: 400;
  @body body: Error;
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
model ConflictResponse {
  @statusCode statusCode: 409;
  @body body: Error;
}

@error
model TooManyRequestsResponse {
  @statusCode statusCode: 429;
  @body body: Error;
}

@error
model ServiceUnavailableResponse {
  @statusCode statusCode: 503;
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

model QueryResult {
  columns: string[];
  rows?: unknown[][];
  row_count?: int64;
  next_page_token?: string;
}

enum QueryJobStatus {
  QUEUED,
  RUNNING,
  SUCCEEDED,
  FAILED,
  CANCELED,
}

model QueryJob {
  query_id: string;
  status: QueryJobStatus;
  row_count: int64;
  request_id?: string;
  error?: string;
  created_at?: utcDateTime;
  started_at?: utcDateTime;
  completed_at?: utcDateTime;
}

model SubmitQueryResponse {
  query_id: string;
  status: QueryJobStatus;
}

model CancelQueryResponse {
  query_id: string;
  status: QueryJobStatus;
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

model AuthPrincipalSummary {
  id: string;
  name: string;
  is_admin: boolean;
}

model AuthLoginResponse {
  token: string;
  principal: AuthPrincipalSummary;
}

model BootstrapCompleteRequest {
  username: string;
  password: string;
  principal_name?: string;
  bootstrap_token?: string;
}

model LocalLoginRequest {
  username: string;
  password: string;
}

model BootstrapTokenRequest {
  ttl_seconds?: int64;
}

model BootstrapTokenResponse {
  bootstrap_token: string;
  ttl_seconds: int64;
}

model OIDCProviderRequest {
  enabled: boolean;
  issuer_url?: string;
  jwks_url?: string;
  audience?: string;
  client_id?: string;
  client_secret?: string;
  scopes?: string;
}

model OIDCProviderResponse {
  enabled: boolean;
  issuer_url?: string;
  jwks_url?: string;
  audience?: string;
  client_id?: string;
  scopes?: string;
  updated_at?: string;
  secret_stored: boolean;
}

model RevokeWebSessionsRequest {
  principal_id: string;
}

model WebSessionStatsResponse {
  created_total: int64;
  resolved_total: int64;
  resolve_failed_total: int64;
  revoked_total: int64;
  revoked_all_total: int64;
  reaped_total: int64;
  active_sessions: int64;
  idle_ttl_seconds: int64;
  absolute_ttl_seconds: int64;
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

enum ComputeAssignmentPrincipalType {
  User: "user";
  Group: "group";
}

enum ComputeEndpointSize {
  SMALL: "SMALL";
  MEDIUM: "MEDIUM";
  LARGE: "LARGE";
}

enum ComputeEndpointStatus {
  ACTIVE: "ACTIVE";
  INACTIVE: "INACTIVE";
  STARTING: "STARTING";
  STOPPING: "STOPPING";
  ERROR: "ERROR";
}

enum ComputeEndpointType {
  LOCAL: "LOCAL";
  REMOTE: "REMOTE";
}

model ComputeAssignment {
  id?: string;
  endpoint_id?: string;
  endpoint_name?: string;
  principal_id?: string;
  principal_type?: ComputeAssignmentPrincipalType;
  fallback_local?: boolean;
  is_default?: boolean;
  created_at?: string;
}

model PaginatedComputeAssignments {
  data: ComputeAssignment[];
  next_page_token?: string;
}

model CreateComputeAssignmentRequest {
  principal_id: string;
  principal_type: ComputeAssignmentPrincipalType;
  fallback_local?: boolean;
  is_default?: boolean;
}

model ComputeEndpoint {
  id?: string;
  name?: string;
  type?: ComputeEndpointType;
  size?: ComputeEndpointSize;
  status?: ComputeEndpointStatus;
  url?: string;
  external_id?: string;
  max_memory_gb?: int64;
  owner?: string;
  created_at?: string;
  updated_at?: string;
}

model PaginatedComputeEndpoints {
  data: ComputeEndpoint[];
  next_page_token?: string;
}

model CreateComputeEndpointRequest {
  name: string;
  type: ComputeEndpointType;
  url: string;
  auth_token?: string;
  max_memory_gb?: int64;
  size?: ComputeEndpointSize;
}

model UpdateComputeEndpointRequest {
  auth_token?: string;
  max_memory_gb?: int64;
  size?: ComputeEndpointSize;
  status?: ComputeEndpointStatus;
  url?: string;
}

model ComputeEndpointHealth {
  endpoint_name?: string;
  status?: string;
  memory_used_mb?: int32;
  max_memory_gb?: int32;
  uptime_seconds?: int32;
  duckdb_version?: string;
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

model PurgeLineageRequest {
  older_than_days: int32;
}

model PurgeLineageResponse {
  deleted_count?: int64;
}

enum ColumnLineageEdgeTransformType {
  DIRECT,
  EXPRESSION,
}

model ColumnLineageEdge {
  id?: int64;
  lineage_edge_id?: string;
  target_column?: string;
  source_schema?: string;
  source_table?: string;
  source_column?: string;
  transform_type?: ColumnLineageEdgeTransformType;
  function?: string;
}

model PaginatedColumnLineageEdges {
  data: ColumnLineageEdge[];
  next_page_token?: string;
}

model LineageEdge {
  id?: string;
  source_table?: string;
  target_table?: string;
  source_schema?: string;
  target_schema?: string;
  edge_type?: string;
  principal_name?: string;
  created_at?: utcDateTime;
}

model PaginatedLineageEdges {
  data: LineageEdge[];
  next_page_token?: string;
}

model LineageNode {
  table_name?: string;
  upstream?: LineageEdge[];
  downstream?: LineageEdge[];
}

model Tag {
  id?: string;
  key?: string;
  value?: string;
  created_by?: string;
  created_at?: utcDateTime;
}

model PaginatedTags {
  data: Tag[];
  next_page_token?: string;
}

model CreateTagRequest {
  key: string;
  value?: string;
}

enum TagAssignmentSecurableType {
  schema,
  table,
  column,
  macro,
}

model TagAssignment {
  id?: string;
  tag_id?: string;
  securable_type?: TagAssignmentSecurableType;
  securable_id?: string;
  column_name?: string;
  assigned_by?: string;
  assigned_at?: utcDateTime;
}

model CreateTagAssignmentRequest {
  securable_type: TagAssignmentSecurableType;
  securable_id: string;
  column_name?: string;
}

model GitRepo {
  id?: string;
  url?: string;
  branch?: string;
  path?: string;
  owner?: string;
  last_sync_at?: utcDateTime;
  last_commit?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedGitRepos {
  data: GitRepo[];
  next_page_token?: string;
}

model CreateGitRepoRequest {
  url: string;
  branch: string;
  path?: string;
  auth_token?: string;
}

model GitSyncResult {
  notebooks_created?: int32;
  notebooks_updated?: int32;
  notebooks_deleted?: int32;
  commit_sha?: string;
}

model Macro {
  id?: string;
  name?: string;
  macro_type?: string;
  parameters?: string[];
  body?: string;
  description?: string;
  catalog_name?: string;
  project_name?: string;
  visibility?: string;
  owner?: string;
  properties?: Record<string>;
  tags?: string[];
  status?: string;
  created_by?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedMacros {
  data: Macro[];
  next_page_token?: string;
}

model CreateMacroRequest {
  name: string;
  body: string;
  macro_type?: string;
  parameters?: string[];
  description?: string;
  catalog_name?: string;
  project_name?: string;
  visibility?: string;
  owner?: string;
  properties?: Record<string>;
  tags?: string[];
  status?: string;
}

model UpdateMacroRequest {
  body?: string;
  description?: string;
  parameters?: string[];
  status?: string;
  catalog_name?: string;
  project_name?: string;
  visibility?: string;
  owner?: string;
  properties?: Record<string>;
  tags?: string[];
}

model MacroRevision {
  id?: string;
  macro_name?: string;
  version?: int32;
  content_hash?: string;
  parameters?: string[];
  body?: string;
  description?: string;
  status?: string;
  created_by?: string;
  created_at?: utcDateTime;
}

model MacroRevisionList {
  data: MacroRevision[];
}

model MacroImpactModel {
  target_table?: string;
  target_schema?: string;
  model_name?: string;
  last_seen_at?: utcDateTime;
}

model MacroImpactList {
  data: MacroImpactModel[];
  next_page_token?: string;
}

model MacroRevisionDiff {
  macro_name?: string;
  from_version?: int32;
  to_version?: int32;
  from_content_hash?: string;
  to_content_hash?: string;
  changed?: boolean;
  parameters_changed?: boolean;
  body_changed?: boolean;
  description_changed?: boolean;
  status_changed?: boolean;
  from_parameters?: string[];
  to_parameters?: string[];
  from_body?: string;
  to_body?: string;
  from_description?: string;
  to_description?: string;
  from_status?: string;
  to_status?: string;
  impact_changed?: boolean;
  impacted_models_added?: MacroImpactModel[];
  impacted_models_removed?: MacroImpactModel[];
  impacted_models_unchanged?: MacroImpactModel[];
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
