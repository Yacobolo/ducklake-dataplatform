using TypeSpec.Http;

namespace Duck;

@route("/healthz")
@get
@useAuth([])
@tag("Health")
@summary("Get health")
op getHealth(): Ok<HealthResponse> | StandardErrorResponses;

@route("/query")
@post
@tag("Queries")
@summary("Execute query")
@doc("Executes a SQL statement synchronously and returns the first page of results in the response body.")
op executeQuery(@body body: QueryRequest): Ok<QueryResult> | MutatingErrorResponses;

@route("/principals")
@get
@tag("Identity")
@summary("List principals")
op listPrincipals(@query max_results?: int32, @query page_token?: string): Ok<PaginatedPrincipals> | StandardErrorResponses;

@route("/principals")
@post
@tag("Identity")
@summary("Create principal")
op createPrincipal(@body body: CreatePrincipalRequest): Created<Principal> | MutatingErrorResponses;

@route("/principals/{principalId}")
@get
@tag("Identity")
@summary("Get principal")
op getPrincipal(principalId: string): Ok<Principal> | ResourceErrorResponses;

@route("/principals/{principalId}")
@delete
@tag("Identity")
@summary("Delete principal")
op deletePrincipal(principalId: string): NoContent | MutatingErrorResponses;

@route("/principals/{principalId}/admin")
@put
@tag("Identity")
@summary("Update principal admin")
op updatePrincipalAdmin(principalId: string, @body body: UpdatePrincipalAdminRequest): NoContent | MutatingErrorResponses;

@route("/groups")
@get
@tag("Identity")
@summary("List groups")
op listGroups(@query max_results?: int32, @query page_token?: string): Ok<PaginatedGroups> | StandardErrorResponses;

@route("/groups")
@post
@tag("Identity")
@summary("Create group")
op createGroup(@body body: CreateGroupRequest): Created<Group> | MutatingErrorResponses;

@route("/groups/{groupId}")
@get
@tag("Identity")
@summary("Get group")
op getGroup(groupId: string): Ok<Group> | ResourceErrorResponses;

@route("/groups/{groupId}")
@patch(#{implicitOptionality: true})
@tag("Identity")
@summary("Update group")
op updateGroup(groupId: string, @body body: UpdateGroupRequest): Ok<Group> | ValidationResourceErrorResponses;

@route("/groups/{groupId}")
@delete
@tag("Identity")
@summary("Delete group")
op deleteGroup(groupId: string): NoContent | MutatingErrorResponses;

@route("/groups/{groupId}/members")
@get
@tag("Identity")
@summary("List group members")
op listGroupMembers(groupId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedGroupMembers> | ResourceErrorResponses;

@route("/groups/{groupId}/members")
@post
@tag("Identity")
@summary("Create group member")
op createGroupMember(groupId: string, @body body: CreateGroupMemberRequest): CreatedEmpty | MutatingErrorResponses;

@route("/groups/{groupId}/members")
@delete
@tag("Identity")
@summary("Delete group member")
op deleteGroupMember(groupId: string, @query member_type: PrincipalType, @query member_id: string): NoContent | MutatingErrorResponses;

@route("/grants")
@get
@tag("Governance")
@summary("List grants")
op listGrants(
  @query principal_id?: string,
  @query principal_type?: PrincipalType,
  @query securable_type?: string,
  @query securable_id?: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedGrants> | StandardErrorResponses;

@route("/grants")
@post
@tag("Governance")
@summary("Create grant")
op createGrant(@body body: CreateGrantRequest): Created<PrivilegeGrant> | MutatingErrorResponses;

@route("/grants/{grantId}")
@delete
@tag("Governance")
@summary("Delete grant")
op deleteGrant(grantId: string): NoContent | MutatingErrorResponses;

@route("/api-keys")
@get
@tag("Identity")
@summary("List API keys")
op listAPIKeys(@query principal_id?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAPIKeys> | StandardErrorResponses;

@route("/api-keys")
@post
@tag("Identity")
@summary("Create API key")
op createAPIKey(@body body: CreateAPIKeyRequest): Created<CreateAPIKeyResponse> | MutatingErrorResponses;

@route("/api-keys/{apiKeyId}")
@delete
@tag("Identity")
@summary("Delete API key")
op deleteAPIKey(apiKeyId: string): NoContent | MutatingErrorResponses;

@route("/api-keys/cleanup")
@post
@tag("Identity")
@summary("Clean up expired API keys")
op cleanupExpiredAPIKeys(): Ok<CleanupAPIKeysResponse> | MutatingErrorResponses;

@route("/tables/{tableId}/row-filters")
@get
@tag("Governance")
@summary("List row filters")
op listRowFilters(tableId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedRowFilters> | ResourceErrorResponses;

@route("/tables/{tableId}/row-filters")
@post
@tag("Governance")
@summary("Create row filter")
op createRowFilter(tableId: string, @body body: CreateRowFilterRequest): Created<RowFilter> | MutatingErrorResponses;

@route("/row-filters/{rowFilterId}")
@delete
@tag("Governance")
@summary("Delete row filter")
op deleteRowFilter(rowFilterId: string): NoContent | MutatingErrorResponses;

@route("/row-filters/{rowFilterId}/bindings")
@get
@tag("Governance")
@summary("List row filter bindings")
op listRowFilterBindings(
  rowFilterId: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedRowFilterBindings> | ResourceErrorResponses;

@route("/row-filters/{rowFilterId}/bindings")
@post
@tag("Governance")
@summary("Bind row filter")
op bindRowFilter(rowFilterId: string, @body body: RowFilterBindingRequest): CreatedEmpty | MutatingErrorResponses;

@route("/row-filters/{rowFilterId}/bindings")
@delete
@tag("Governance")
@summary("Unbind row filter")
op unbindRowFilter(rowFilterId: string, @query principal_id: string, @query principal_type: PrincipalType): NoContent | MutatingErrorResponses;

@route("/tables/{tableId}/column-masks")
@get
@tag("Governance")
@summary("List column masks")
op listColumnMasks(tableId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnMasks> | ResourceErrorResponses;

@route("/tables/{tableId}/column-masks")
@post
@tag("Governance")
@summary("Create column mask")
op createColumnMask(tableId: string, @body body: CreateColumnMaskRequest): Created<ColumnMask> | MutatingErrorResponses;

@route("/column-masks/{columnMaskId}")
@delete
@tag("Governance")
@summary("Delete column mask")
op deleteColumnMask(columnMaskId: string): NoContent | MutatingErrorResponses;

@route("/column-masks/{columnMaskId}/bindings")
@get
@tag("Governance")
@summary("List column mask bindings")
op listColumnMaskBindings(
  columnMaskId: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedColumnMaskBindings> | ResourceErrorResponses;

@route("/column-masks/{columnMaskId}/bindings")
@post
@tag("Governance")
@summary("Bind column mask")
op bindColumnMask(columnMaskId: string, @body body: ColumnMaskBindingRequest): CreatedEmpty | MutatingErrorResponses;

@route("/column-masks/{columnMaskId}/bindings")
@delete
@tag("Governance")
@summary("Unbind column mask")
op unbindColumnMask(columnMaskId: string, @query principal_id: string, @query principal_type: PrincipalType): NoContent | MutatingErrorResponses;

@route("/manifest")
@post
@tag("Catalogs")
@summary("Create manifest")
op createManifest(@body body: ManifestRequest): Ok<ManifestResponse> | MutatingErrorResponses;

@route("/audit-logs")
@get
@tag("Queries")
@summary("List audit logs")
op listAuditLogs(
  @query principal_name?: string,
  @query action?: string,
  @query status?: AuditDecisionStatus,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedAuditLogs> | StandardErrorResponses;

@route("/catalogs/{catalogName}/metastore/summary")
@get
@tag("Catalogs")
@summary("Get metastore summary")
op getMetastoreSummary(catalogName: string): Ok<MetastoreSummary> | ResourceErrorResponses;

@route("/query-history")
@get
@tag("Queries")
@summary("List query history")
@doc("Lists recorded query executions and supports filtering by principal, decision status, and time window.")
op listQueryHistory(
  @query principal_name?: string,
  @query status?: AuditDecisionStatus,
  @query from?: utcDateTime,
  @query to?: utcDateTime,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedQueryHistoryEntries> | StandardErrorResponses;

@route("/catalogs")
@post
@tag("Catalogs")
@summary("Register catalog")
op registerCatalog(@body body: CreateCatalogRequest): Created<CatalogRegistration> | MutatingErrorResponses;

@route("/catalogs")
@get
@tag("Catalogs")
@summary("List catalogs")
@doc("Lists registered catalogs and returns a paginated catalog registration view for management clients.")
op listCatalogs(@query max_results?: int32, @query page_token?: string): Ok<CatalogRegistrationList> | StandardErrorResponses;

@route("/catalogs/{catalogName}")
@get
@tag("Catalogs")
@summary("Get catalog registration")
op getCatalogRegistration(catalogName: string): Ok<CatalogRegistration> | ResourceErrorResponses;

@route("/catalogs/{catalogName}")
@patch(#{ implicitOptionality: true })
@tag("Catalogs")
@summary("Update catalog registration")
op updateCatalogRegistration(catalogName: string, @body body: UpdateCatalogRegistrationRequest): Ok<CatalogRegistration> | ResourceErrorResponses;

@route("/catalogs/{catalogName}")
@delete
@tag("Catalogs")
@summary("Delete catalog registration")
op deleteCatalogRegistration(catalogName: string): NoContent | ResourceErrorResponses;

@route("/catalogs/{catalogName}/set-default")
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
