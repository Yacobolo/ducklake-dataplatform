using TypeSpec.Http;

namespace Duck;

alias MetadataMap = Record<string>;
alias JsonObject = Record<unknown>;

enum PrincipalType {
  user,
  group,
}

enum AuditDecisionStatus {
  ALLOWED,
  DENIED,
  ERROR,
}

enum CatalogStatus {
  ACTIVE,
  ERROR,
  DETACHED,
}

enum MetastoreType {
  sqlite,
  postgres,
}

enum MacroType {
  SCALAR,
  TABLE,
}

enum MacroVisibility {
  project,
  catalog_global,
  system,
}

enum MacroStatus {
  ACTIVE,
  DEPRECATED,
}

enum StorageCredentialType {
  S3,
  AZURE,
  GCS,
}

enum StorageType {
  S3,
  AZURE,
  GCS,
}

enum URLStyle {
  path,
  vhost,
}

@summary("Metadata for a result-set column.")
model TabularColumn {
  name: string;
}

@error
@summary("Standard API error response.")
@doc("Errors use a shared schema across the API so clients can handle failure responses consistently.")
model Error {
  code: int32;
  message: string;
  details?: MetadataMap;
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

@error
model BadGatewayResponse {
  @statusCode statusCode: 502;
  @body body: Error;
}

@error
model NotImplementedResponse {
  @statusCode statusCode: 501;
  @body body: Error;
}

alias StandardErrorResponses = BadRequestResponse | UnauthorizedResponse | TooManyRequestsResponse | InternalServerErrorResponse;
alias GuardedReadErrorResponses = StandardErrorResponses | ForbiddenResponse;
alias MutatingErrorResponses = StandardErrorResponses | ForbiddenResponse;
alias ResourceErrorResponses = MutatingErrorResponses | NotFoundResponse;
alias ValidationResourceErrorResponses = ResourceErrorResponses | BadRequestResponse;
alias LookupErrorResponses = StandardErrorResponses | NotFoundResponse;

@summary("Service health status.")
model HealthResponse {
  status: string;
}

@summary("Synchronous SQL query request.")
@doc("Submits a SQL statement for immediate execution and returns a tabular result when the request completes.")
model QueryRequest {
  sql: string;
}

@summary("Tabular SQL query result.")
@doc("Contains result-set columns, row data, and an optional continuation token when additional rows are available.")
model QueryResult {
  columns: TabularColumn[];
  rows: JsonObject[];
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

@summary("Authenticated principal.")
model Principal {
  id: string;
  name: string;
  type: PrincipalType;
  is_admin: boolean;
  created_at?: utcDateTime;
}

model PaginatedPrincipals {
  data: Principal[];
  next_page_token?: string;
}

model CreatePrincipalRequest {
  name: string;
  type?: PrincipalType;
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
  updated_at?: utcDateTime;
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
  created_at?: utcDateTime;
}

model PaginatedGroups {
  data: Group[];
  next_page_token?: string;
}

model CreateGroupRequest {
  name: string;
  description?: string;
}

model UpdateGroupRequest {
  description?: string;
}

model GroupMember {
  group_id: string;
  member_type: PrincipalType;
  member_id: string;
}

model PaginatedGroupMembers {
  data: GroupMember[];
  next_page_token?: string;
}

model CreateGroupMemberRequest {
  member_type: PrincipalType;
  member_id: string;
}

model PrivilegeGrant {
  id: string;
  principal_id: string;
  principal_type: PrincipalType;
  securable_type: string;
  securable_id: string;
  privilege: string;
  granted_by?: string;
  granted_at?: utcDateTime;
}

model PaginatedGrants {
  data: PrivilegeGrant[];
  next_page_token?: string;
}

model CreateGrantRequest {
  principal_id: string;
  principal_type: PrincipalType;
  securable_type: string;
  securable_id: string;
  privilege: string;
}

@summary("API key metadata.")
@doc("Represents a stored API key without returning the full secret value.")
model APIKeyInfo {
  id: string;
  principal_id: string;
  name: string;
  key_prefix?: string;
  expires_at?: utcDateTime;
  created_at?: utcDateTime;
}

model PaginatedAPIKeys {
  data: APIKeyInfo[];
  next_page_token?: string;
}

model CreateAPIKeyRequest {
  principal_id: string;
  name?: string;
  expires_at?: utcDateTime;
}

model CreateAPIKeyResponse {
  id: string;
  key: string;
  name?: string;
  key_prefix?: string;
  expires_at?: utcDateTime;
  created_at?: utcDateTime;
}

model CleanupAPIKeysResponse {
  deleted_count: int32;
}

model RowFilter {
  id: string;
  table_id: string;
  name: string;
  filter_sql: string;
  description?: string;
  created_at?: utcDateTime;
}

model PaginatedRowFilters {
  data: RowFilter[];
  next_page_token?: string;
}

model CreateRowFilterRequest {
  table_id?: string;
  name: string;
  filter_sql: string;
  description?: string;
}

model RowFilterBindingRequest {
  principal_id: string;
  principal_type: PrincipalType;
}

model RowFilterBinding {
  id?: string;
  row_filter_id?: string;
  principal_id?: string;
  principal_type?: PrincipalType;
}

model PaginatedRowFilterBindings {
  data: RowFilterBinding[];
  next_page_token?: string;
}

model ColumnMask {
  id: string;
  table_id: string;
  name: string;
  column_name: string;
  mask_expression: string;
  description?: string;
  created_at?: utcDateTime;
}

model PaginatedColumnMasks {
  data: ColumnMask[];
  next_page_token?: string;
}

model CreateColumnMaskRequest {
  name: string;
  column_name: string;
  mask_expression: string;
  description?: string;
}

model ColumnMaskBindingRequest {
  principal_id: string;
  principal_type: PrincipalType;
  see_original?: boolean;
}

model ColumnMaskBinding {
  id?: string;
  column_mask_id?: string;
  principal_id?: string;
  principal_type?: PrincipalType;
  see_original?: boolean;
}

model PaginatedColumnMaskBindings {
  data: ColumnMaskBinding[];
  next_page_token?: string;
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
  column_masks?: MetadataMap;
  expires_at?: utcDateTime;
}

model AuditEntry {
  id: string;
  principal_name?: string;
  action?: string;
  statement_type?: string;
  original_sql?: string;
  rewritten_sql?: string;
  tables_accessed?: string[];
  status?: AuditDecisionStatus;
  error_message?: string;
  duration_ms?: int64;
  created_at?: utcDateTime;
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
  status?: AuditDecisionStatus;
  error_message?: string;
  duration_ms?: int64;
  rows_returned?: int64;
  created_at?: utcDateTime;
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
  metastore_type?: MetastoreType;
  dsn?: string;
  data_path?: string;
  status?: CatalogStatus;
  is_default?: boolean;
  comment?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
  system_managed?: boolean;
}

model CatalogRegistrationList {
  catalogs: CatalogRegistration[];
  next_page_token?: string;
  total_count?: int32;
}

model CreateCatalogRequest {
  name: string;
  metastore_type?: MetastoreType;
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
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
  system_managed?: boolean;
}

model VersionedObjectSummary {
  total_count?: int64;
  active_count?: int64;
  historical_count?: int64;
  has_history?: boolean;
  latest_snapshot_id?: int64;
}

model CatalogVersionSummary {
  catalog_name?: string;
  version?: string;
  created_by?: string;
  encrypted?: boolean;
  data_path?: string;
  latest_snapshot_id?: int64;
  schemas?: VersionedObjectSummary;
  tables?: VersionedObjectSummary;
  columns?: VersionedObjectSummary;
}

model CatalogHistoryEntry {
  entity_type?: string;
  schema_name?: string;
  table_name?: string;
  column_name?: string;
  object_name?: string;
  object_id?: string;
  begin_snapshot_id?: int64;
  end_snapshot_id?: int64;
  latest_snapshot_id?: int64;
  is_active?: boolean;
  has_history?: boolean;
}

model CatalogHistoryResponse {
  data: CatalogHistoryEntry[];
}

model SchemaDetail {
  schema_id: string;
  name: string;
  catalog_name: string;
  comment?: string;
  properties?: MetadataMap;
  tags?: Tag[];
  owner?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedSchemaDetails {
  data: SchemaDetail[];
  next_page_token?: string;
}

model CreateSchemaRequest {
  name: string;
  comment?: string;
  location_name?: string;
  properties?: MetadataMap;
}

model UpdateSchemaRequest {
  comment?: string;
  properties?: MetadataMap;
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
  properties?: MetadataMap;
  owner?: string;
  tags?: Tag[];
  statistics?: TableStatistics;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
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
  properties?: MetadataMap;
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
  last_profiled_at?: utcDateTime;
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
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
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
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
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
  created_at?: utcDateTime;
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
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedComputeEndpoints {
  data: ComputeEndpoint[];
  next_page_token?: string;
}

model CreateComputeEndpointRequest {
  name: string;
  type: ComputeEndpointType;
  @doc("Endpoint URI. REMOTE endpoints must use grpc:// or grpcs://; LOCAL endpoints use local routing URLs.")
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

model ComputeRoutingDefaults {
  interactive_mode?: string;
  scheduled_mode?: string;
  notebook_mode?: string;
}

model SubmitQueryRequest {
  sql: string;
  request_id?: string;
}

enum ModelMaterialization {
  VIEW,
  TABLE,
  INCREMENTAL,
  EPHEMERAL,
  SEED,
  SNAPSHOT,
}

enum ModelConfigOnSchemaChange {
  ignore,
  fail,
}

model ModelConfig {
  unique_key?: string[];
  incremental_strategy?: string;
  on_schema_change?: ModelConfigOnSchemaChange;
}

model ModelContractColumn {
  name: string;
  type: string;
  nullable?: boolean;
}

model ModelContract {
  enforce?: boolean;
  columns?: ModelContractColumn[];
}

model FreshnessPolicy {
  max_lag_seconds?: int64;
  cron_schedule?: string;
}

model CreateModelRequest {
  project_name: string;
  name: string;
  sql: string;
  materialization?: ModelMaterialization;
  description?: string;
  tags?: string[];
  config?: ModelConfig;
  contract?: ModelContract;
  freshness_policy?: FreshnessPolicy;
}

model UpdateModelRequest {
  sql?: string;
  materialization?: ModelMaterialization;
  description?: string;
  tags?: string[];
  config?: ModelConfig;
  contract?: ModelContract;
  freshness_policy?: FreshnessPolicy;
}

model Model {
  id?: string;
  project_name?: string;
  name?: string;
  sql?: string;
  materialization?: ModelMaterialization;
  description?: string;
  owner?: string;
  depends_on?: string[];
  tags?: string[];
  config?: ModelConfig;
  contract?: ModelContract;
  freshness_policy?: FreshnessPolicy;
  created_by?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedModels {
  data: Model[];
  next_page_token?: string;
}

model ModelDAGNode {
  project_name?: string;
  model_name?: string;
  materialization?: ModelMaterialization;
  depends_on?: string[];
}

model ModelDAGTier {
  tier?: int32;
  nodes?: ModelDAGNode[];
}

model ModelDAG {
  tiers?: ModelDAGTier[];
}

enum ModelTestTestType {
  not_null,
  unique,
  accepted_values,
  relationships,
  custom_sql,
}

model ModelTestConfig {
  values?: string[];
  to_model?: string;
  to_column?: string;
  custom_sql?: string;
}

model CreateModelTestRequest {
  name: string;
  test_type: ModelTestTestType;
  column?: string;
  config?: ModelTestConfig;
}

model ModelTest {
  id?: string;
  model_id?: string;
  name?: string;
  test_type?: ModelTestTestType;
  column?: string;
  config?: ModelTestConfig;
  created_at?: utcDateTime;
}

model ModelTestList {
  data: ModelTest[];
}

enum ModelTestResultStatus {
  PASS,
  FAIL,
  ERROR,
}

model ModelTestResult {
  id?: string;
  run_step_id?: string;
  test_id?: string;
  test_name?: string;
  status?: ModelTestResultStatus;
  rows_returned?: int64;
  error_message?: string;
  created_at?: utcDateTime;
}

model ModelTestResultList {
  data: ModelTestResult[];
}

model FreshnessStatus {
  is_fresh?: boolean;
  last_run_at?: utcDateTime;
  max_lag_seconds?: int64;
  stale_since?: utcDateTime;
}

model SourceFreshnessStatus {
  is_fresh?: boolean;
  source_schema?: string;
  source_table?: string;
  timestamp_column?: string;
  last_loaded_at?: utcDateTime;
  max_lag_seconds?: int64;
  stale_since?: utcDateTime;
}

model PromoteNotebookRequest {
  notebook_id: string;
  cell_index: int32;
  project_name: string;
  name: string;
  materialization?: ModelMaterialization;
}

model CreateNotebookRequest {
  name: string;
  description?: string;
  source?: string;
  folder_id?: string;
}

model UpdateNotebookRequest {
  name?: string;
  description?: string;
  project_override_id?: string;
  environment_override_id?: string;
}

model Notebook {
  id?: string;
  folder_id?: string;
  name?: string;
  description?: string;
  owner?: string;
  git_repo_id?: string;
  git_path?: string;
  project_override_id?: string;
  environment_override_id?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedNotebooks {
  data: Notebook[];
  next_page_token?: string;
}

enum NotebookShareRole {
  viewer,
  editor,
  manager,
}

model NotebookContext {
  notebook_id?: string;
  folder_id?: string;
  effective_project_id?: string;
  effective_environment_id?: string;
  effective_git_repo_id?: string;
  effective_git_root_path?: string;
  project_source_folder_id?: string;
  environment_source_id?: string;
  git_source_folder_id?: string;
}

model NotebookShare {
  principal_name?: string;
  role?: NotebookShareRole;
}

model Folder {
  id?: string;
  name?: string;
  owner?: string;
  parent_folder_id?: string;
  path?: string;
  depth?: int32;
  system_role?: string;
  git_repo_id?: string;
  git_root_path?: string;
  default_project_id?: string;
  default_environment_id?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model FolderShare {
  principal_name?: string;
  role?: NotebookShareRole;
}

model PaginatedFolders {
  data: Folder[];
  next_page_token?: string;
}

model CreateFolderRequest {
  name: string;
  parent_folder_id?: string;
  git_repo_id?: string;
  git_root_path?: string;
  default_project_id?: string;
  default_environment_id?: string;
}

model UpdateFolderRequest {
  name?: string;
  git_repo_id?: string;
  git_root_path?: string;
  default_project_id?: string;
  default_environment_id?: string;
}

model MoveFolderRequest {
  parent_folder_id?: string;
  confirm_leave_git?: boolean;
  confirm_context_change?: boolean;
}

model MoveNotebookRequest {
  folder_id: string;
  git_path?: string;
  confirm_leave_git?: boolean;
  confirm_context_change?: boolean;
}

model DuplicateNotebookRequest {
  folder_id: string;
  name?: string;
  git_path?: string;
}

model ShareNotebookRequest {
  principal_name: string;
  role?: NotebookShareRole;
}

model ShareFolderRequest {
  principal_name: string;
  role?: NotebookShareRole;
}

enum VisualOutputKind {
  table,
  metric,
  chart,
}

enum VisualChartType {
  bar,
  line,
  area,
  pie,
  doughnut,
  scatter,
  stacked_bar,
}

model VisualFieldBinding {
  field: string;
}

model VisualEncodings {
  x?: VisualFieldBinding;
  y?: VisualFieldBinding;
  series?: VisualFieldBinding;
  label?: VisualFieldBinding;
  value?: VisualFieldBinding;
  secondary?: VisualFieldBinding;
}

model VisualSpec {
  kind: VisualOutputKind;
  chart_type?: VisualChartType;
  encodings?: VisualEncodings;
  title?: string;
  subtitle?: string;
  legend?: boolean;
  stacked?: boolean;
  color_palette?: string;
}

enum CellCellType {
  sql,
  markdown,
}

enum CellRole {
  transform,
  output,
  test,
  markdown,
}

enum NotebookTestSeverity {
  error,
  warn,
}

model NotebookCellTestConfig {
  severity?: NotebookTestSeverity;
}

model CreateCellRequest {
  cell_type: CellCellType;
  name?: string;
  role?: CellRole;
  disabled?: boolean;
  test?: NotebookCellTestConfig;
  visual_spec?: VisualSpec;
  content?: string;
  position?: int32;
}

model UpdateCellRequest {
  name?: string;
  role?: CellRole;
  disabled?: boolean;
  test?: NotebookCellTestConfig;
  visual_spec?: VisualSpec;
  content?: string;
  position?: int32;
}

model Cell {
  id?: string;
  notebook_id?: string;
  cell_type?: CellCellType;
  name?: string;
  role?: CellRole;
  disabled?: boolean;
  test?: NotebookCellTestConfig;
  visual_spec?: VisualSpec;
  content?: string;
  position?: int32;
  last_result?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model CellList {
  data: Cell[];
}

model ReorderCellsRequest {
  cell_ids: string[];
}

model NotebookDetail {
  notebook?: Notebook;
  cells?: Cell[];
  context?: NotebookContext;
  shares?: NotebookShare[];
  publish_model?: NotebookPublishModel;
}

model NotebookPublishModel {
  project_name?: string;
  name?: string;
  materialization?: ModelMaterialization;
  output_cell_id?: string;
}

enum NotebookSessionState {
  active,
  closed,
}

model NotebookSession {
  id?: string;
  notebook_id?: string;
  principal?: string;
  state?: NotebookSessionState;
  created_at?: utcDateTime;
  last_used_at?: utcDateTime;
}

model CellExecutionResult {
  cell_id?: string;
  columns?: TabularColumn[];
  rows?: JsonObject[];
  row_count?: int32;
  error?: string;
  duration_ms?: int64;
}

model RunAllResult {
  notebook_id?: string;
  results?: CellExecutionResult[];
  total_duration_ms?: int64;
}

enum NotebookJobState {
  pending,
  running,
  complete,
  failed,
}

model NotebookJob {
  id?: string;
  notebook_id?: string;
  session_id?: string;
  state?: NotebookJobState;
  result?: string;
  error?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedNotebookJobs {
  data: NotebookJob[];
  next_page_token?: string;
}

model CreatePipelineRequest {
  name: string;
  description?: string;
  schedule_cron?: string;
  is_paused?: boolean;
  concurrency_limit?: int32;
  folder_id?: string;
}

model UpdatePipelineRequest {
  description?: string;
  schedule_cron?: string;
  is_paused?: boolean;
  concurrency_limit?: int32;
  folder_id?: string;
}

model Pipeline {
  id?: string;
  name?: string;
  description?: string;
  schedule_cron?: string;
  is_paused?: boolean;
  concurrency_limit?: int32;
  created_by?: string;
  folder_id?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedPipelines {
  data: Pipeline[];
  next_page_token?: string;
}

enum PipelineJobJobType {
  NOTEBOOK,
  MODEL_RUN,
}

model CreatePipelineJobRequest {
  name: string;
  notebook_id?: string;
  compute_endpoint_id?: string;
  depends_on?: string[];
  timeout_seconds?: int64;
  retry_count?: int32;
  job_order?: int32;
  job_type?: PipelineJobJobType;
  model_selector?: string;
}

model PipelineJob {
  id?: string;
  pipeline_id?: string;
  name?: string;
  notebook_id?: string;
  compute_endpoint_id?: string;
  depends_on?: string[];
  timeout_seconds?: int64;
  retry_count?: int32;
  job_order?: int32;
  job_type?: PipelineJobJobType;
  model_selector?: string;
  created_at?: utcDateTime;
}

model PipelineJobList {
  data: PipelineJob[];
}

enum PipelineRunStatus {
  PENDING,
  RUNNING,
  SUCCESS,
  FAILED,
  CANCELLED,
}

enum PipelineRunTriggerType {
  MANUAL,
  SCHEDULED,
}

model TriggerPipelineRunRequest {
  parameters?: MetadataMap;
}

model PipelineRun {
  id?: string;
  pipeline_id?: string;
  status?: PipelineRunStatus;
  trigger_type?: PipelineRunTriggerType;
  triggered_by?: string;
  parameters?: MetadataMap;
  git_commit_hash?: string;
  started_at?: utcDateTime;
  finished_at?: utcDateTime;
  error_message?: string;
  created_at?: utcDateTime;
}

model PaginatedPipelineRuns {
  data: PipelineRun[];
  next_page_token?: string;
}

enum PipelineJobRunStatus {
  PENDING,
  RUNNING,
  SUCCESS,
  FAILED,
  SKIPPED,
  CANCELLED,
}

model PipelineJobRun {
  id?: string;
  run_id?: string;
  job_id?: string;
  job_name?: string;
  status?: PipelineJobRunStatus;
  started_at?: utcDateTime;
  finished_at?: utcDateTime;
  error_message?: string;
  retry_attempt?: int32;
  created_at?: utcDateTime;
}

model PipelineJobRunList {
  data: PipelineJobRun[];
}

model SearchResult {
  type?: string;
  name?: string;
  schema_name?: string;
  table_name?: string;
  comment?: string;
  match_field?: string;
}

model PaginatedSearchResults {
  data: SearchResult[];
  next_page_token?: string;
}

model CreateSemanticModelRequest {
  project_name: string;
  name: string;
  description?: string;
  base_model_ref: string;
  default_time_dimension?: string;
  tags?: string[];
}

model UpdateSemanticModelRequest {
  description?: string;
  owner?: string;
  base_model_ref?: string;
  default_time_dimension?: string;
  tags?: string[];
}

model SemanticModel {
  id?: string;
  project_name?: string;
  name?: string;
  description?: string;
  owner?: string;
  base_model_ref?: string;
  default_time_dimension?: string;
  tags?: string[];
  created_by?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedSemanticModels {
  data: SemanticModel[];
  next_page_token?: string;
}

enum SemanticMetricMetricType {
  SUM,
  COUNT,
  COUNT_DISTINCT,
  AVG,
  MIN,
  MAX,
  RATIO,
}

enum SemanticMetricExpressionMode {
  DSL,
  SQL,
}

enum CreateSemanticMetricRequestCertificationState {
  DRAFT,
  CERTIFIED,
  DEPRECATED,
}

alias SemanticMetricCertificationState = CreateSemanticMetricRequestCertificationState;

model CreateSemanticMetricRequest {
  name: string;
  description?: string;
  label?: string;
  metric_type: SemanticMetricMetricType;
  expression_mode?: SemanticMetricExpressionMode;
  expression: string;
  filter_sql?: string;
  default_time_grain?: string;
  format?: string;
  certification_state?: CreateSemanticMetricRequestCertificationState;
}

model UpdateSemanticMetricRequest {
  description?: string;
  label?: string;
  metric_type?: SemanticMetricMetricType;
  expression_mode?: SemanticMetricExpressionMode;
  expression?: string;
  filter_sql?: string;
  default_time_grain?: string;
  format?: string;
  owner?: string;
  certification_state?: SemanticMetricCertificationState;
}

model SemanticMetric {
  id?: string;
  semantic_model_id?: string;
  name?: string;
  description?: string;
  label?: string;
  metric_type?: SemanticMetricMetricType;
  expression_mode?: SemanticMetricExpressionMode;
  expression?: string;
  filter_sql?: string;
  default_time_grain?: string;
  format?: string;
  owner?: string;
  certification_state?: SemanticMetricCertificationState;
  created_by?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model SemanticMetricList {
  data: SemanticMetric[];
}

model CreateSemanticPreAggregationRequest {
  name: string;
  metric_set?: string[];
  dimension_set?: string[];
  grain?: string;
  target_relation: string;
  refresh_policy?: string;
}

model UpdateSemanticPreAggregationRequest {
  metric_set?: string[];
  dimension_set?: string[];
  grain?: string;
  target_relation?: string;
  refresh_policy?: string;
}

model SemanticPreAggregation {
  id?: string;
  semantic_model_id?: string;
  name?: string;
  metric_set?: string[];
  dimension_set?: string[];
  grain?: string;
  target_relation?: string;
  refresh_policy?: string;
  created_by?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model SemanticPreAggregationList {
  data: SemanticPreAggregation[];
}

enum SemanticRelationshipRelationshipType {
  ONE_TO_ONE,
  ONE_TO_MANY,
  MANY_TO_ONE,
  MANY_TO_MANY,
}

model CreateSemanticRelationshipRequest {
  name: string;
  from_semantic_id: string;
  to_semantic_id: string;
  relationship_type: SemanticRelationshipRelationshipType;
  join_sql: string;
  is_default?: boolean;
  cost?: int32;
  max_hops?: int32;
}

model UpdateSemanticRelationshipRequest {
  relationship_type?: SemanticRelationshipRelationshipType;
  join_sql?: string;
  is_default?: boolean;
  cost?: int32;
  max_hops?: int32;
}

model SemanticRelationship {
  id?: string;
  name?: string;
  from_semantic_id?: string;
  to_semantic_id?: string;
  relationship_type?: SemanticRelationshipRelationshipType;
  join_sql?: string;
  is_default?: boolean;
  cost?: int32;
  max_hops?: int32;
  created_by?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedSemanticRelationships {
  data: SemanticRelationship[];
  next_page_token?: string;
}

model SemanticRelationshipList {
  data: SemanticRelationship[];
}

model TriggerModelRunRequest {
  project_name: string;
  environment_name?: string;
  model_names?: string[];
  full_refresh?: boolean;
}

model ModelRunCompileDiagnostics {
  warnings?: string[];
  errors?: string[];
}

model ModelRun {
  id?: string;
  status?: string;
  trigger_type?: string;
  triggered_by?: string;
  project_name?: string;
  environment_name?: string;
  build_id?: string;
  model_names?: string[];
  full_refresh?: boolean;
  compile_manifest?: string;
  compile_diagnostics?: ModelRunCompileDiagnostics;
  started_at?: utcDateTime;
  finished_at?: utcDateTime;
  error_message?: string;
  created_at?: utcDateTime;
}

model PaginatedModelRuns {
  data: ModelRun[];
  next_page_token?: string;
}

model ModelRunStep {
  id?: string;
  run_id?: string;
  model_name?: string;
  compiled_sql?: string;
  compiled_hash?: string;
  depends_on?: string[];
  vars_used?: string[];
  macros_used?: string[];
  status?: string;
  rows_affected?: int64;
  started_at?: utcDateTime;
  finished_at?: utcDateTime;
  error_message?: string;
  created_at?: utcDateTime;
}

model ModelRunStepList {
  data: ModelRunStep[];
}

model MetricQueryRequest {
  project_name: string;
  semantic_model_name: string;
  metrics: string[];
  dimensions?: string[];
  filters?: string[];
  order_by?: string[];
  limit?: int32;
  time_grain?: string;
}

model MetricQueryJoinStep {
  relationship_name?: string;
  from_model?: string;
  to_model?: string;
  relationship_type?: string;
  join_sql?: string;
}

model MetricQueryPlan {
  base_model_name?: string;
  base_relation?: string;
  metrics?: string[];
  dimensions?: string[];
  time_grain?: string;
  join_path?: MetricQueryJoinStep[];
  selected_pre_aggregation?: string;
  generated_sql?: string;
  freshness_status?: string;
  freshness_basis?: string[];
}

model MetricQueryExplainResponse {
  plan?: MetricQueryPlan;
}

model MetricQueryRunResponse {
  plan?: MetricQueryPlan;
  result?: QueryResult;
}

model MetricFreshnessStatus {
  metric_name?: string;
  project_name?: string;
  semantic_model_name?: string;
  freshness_status?: string;
  freshness_basis?: string[];
  selected_pre_aggregation?: string;
  checked_at?: utcDateTime;
}

model Dashboard {
  id?: string;
  name?: string;
  description?: string;
  owner?: string;
  folder_id?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedDashboards {
  data: Dashboard[];
  next_page_token?: string;
}

enum DashboardWidgetSourceKind {
  sql_query,
  notebook_cell,
  semantic_query,
}

model DashboardWidgetLayout {
  x: int32;
  y: int32;
  w: int32;
  h: int32;
}

model DashboardSQLQuerySource {
  sql: string;
  catalog?: string;
  schema?: string;
}

model DashboardNotebookCellSource {
  notebook_id: string;
  cell_id: string;
}

model DashboardSemanticQuerySource {
  project_name: string;
  semantic_model_name: string;
  metrics: string[];
  dimensions?: string[];
  filters?: string[];
  order_by?: string[];
  limit?: int32;
  time_grain?: string;
}

model DashboardWidgetSource {
  kind: DashboardWidgetSourceKind;
  sql_query?: DashboardSQLQuerySource;
  notebook_cell?: DashboardNotebookCellSource;
  semantic_query?: DashboardSemanticQuerySource;
}

model DashboardWidget {
  id?: string;
  dashboard_id?: string;
  name?: string;
  description?: string;
  source?: DashboardWidgetSource;
  visual_spec?: VisualSpec;
  layout?: DashboardWidgetLayout;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model DashboardDetail {
  dashboard?: Dashboard;
  widgets?: DashboardWidget[];
}

model ResolvedDashboardWidget {
  widget?: DashboardWidget;
  columns: string[];
  rows?: unknown[][];
  row_count?: int64;
  generated_sql?: string;
}

model ResolvedDashboardDetail {
  dashboard?: Dashboard;
  widgets?: ResolvedDashboardWidget[];
}

model CreateDashboardRequest {
  name: string;
  description?: string;
  folder_id?: string;
}

model UpdateDashboardRequest {
  name?: string;
  description?: string;
  folder_id?: string;
}

model ExploreItem {
  kind?: string;
  scope?: string;
  id?: string;
  name?: string;
  owner?: string;
  folder_id?: string;
  project_name?: string;
  updated_at?: utcDateTime;
  git_repo_id?: string;
  shared?: boolean;
  project_bound?: boolean;
}

model PaginatedExploreItems {
  data: ExploreItem[];
  next_page_token?: string;
}

model CreateDashboardWidgetRequest {
  name: string;
  description?: string;
  source: DashboardWidgetSource;
  visual_spec?: VisualSpec;
  layout: DashboardWidgetLayout;
}

model UpdateDashboardWidgetRequest {
  name?: string;
  description?: string;
  source?: DashboardWidgetSource;
  visual_spec?: VisualSpec;
  layout?: DashboardWidgetLayout;
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

model PaginatedTagAssignments {
  data: TagAssignment[];
  next_page_token?: string;
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
  macro_type?: MacroType;
  parameters?: string[];
  body?: string;
  description?: string;
  catalog_name?: string;
  project_name?: string;
  visibility?: MacroVisibility;
  owner?: string;
  properties?: MetadataMap;
  tags?: string[];
  status?: MacroStatus;
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
  macro_type?: MacroType;
  parameters?: string[];
  description?: string;
  catalog_name?: string;
  project_name?: string;
  visibility?: MacroVisibility;
  owner?: string;
  properties?: MetadataMap;
  tags?: string[];
  status?: MacroStatus;
}

model UpdateMacroRequest {
  body?: string;
  description?: string;
  parameters?: string[];
  status?: MacroStatus;
  catalog_name?: string;
  project_name?: string;
  visibility?: MacroVisibility;
  owner?: string;
  properties?: MetadataMap;
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
  status?: MacroStatus;
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
  from_status?: MacroStatus;
  to_status?: MacroStatus;
  impact_changed?: boolean;
  impacted_models_added?: MacroImpactModel[];
  impacted_models_removed?: MacroImpactModel[];
  impacted_models_unchanged?: MacroImpactModel[];
}

model StorageCredential {
  id: string;
  name: string;
  credential_type?: StorageCredentialType;
  endpoint?: string;
  region?: string;
  url_style?: URLStyle;
  comment?: string;
  owner?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedStorageCredentials {
  data: StorageCredential[];
  next_page_token?: string;
}

model CreateStorageCredentialRequest {
  name: string;
  credential_type?: StorageCredentialType;
  key_id?: string;
  secret?: string;
  endpoint?: string;
  region?: string;
  url_style?: URLStyle;
  comment?: string;
}

model UpdateStorageCredentialRequest {
  key_id?: string;
  secret?: string;
  endpoint?: string;
  region?: string;
  url_style?: URLStyle;
  comment?: string;
}

model ExternalLocation {
  id: string;
  name: string;
  url: string;
  credential_name?: string;
  storage_type?: StorageType;
  comment?: string;
  owner?: string;
  read_only?: boolean;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model PaginatedExternalLocations {
  data: ExternalLocation[];
  next_page_token?: string;
}

model CreateExternalLocationRequest {
  name: string;
  url: string;
  credential_name?: string;
  storage_type?: StorageType;
  comment?: string;
  read_only?: boolean;
}

model UpdateExternalLocationRequest {
  url?: string;
  credential_name?: string;
  comment?: string;
  read_only?: boolean;
}
