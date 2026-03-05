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

@route("/healthz")
@get
op getHealth(): HealthResponse;

@route("/v1/query")
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
