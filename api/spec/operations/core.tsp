using TypeSpec.Http;

namespace Duck;

@route("/healthz")
@get
op getHealth(): Ok<HealthResponse> | StandardErrorResponses;

@route("/query")
@post
op executeQuery(@body body: QueryRequest): Ok<QueryResult> | MutatingErrorResponses;

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
@get
op listRowFilterBindings(
  rowFilterId: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedRowFilterBindings> | ResourceErrorResponses;

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
@get
op listColumnMaskBindings(
  columnMaskId: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedColumnMaskBindings> | ResourceErrorResponses;

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

@route("/catalogs/{catalogName}/version-summary")
@get
op getCatalogVersionSummary(catalogName: string): Ok<CatalogVersionSummary> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/history")
@get
op listCatalogHistory(
  catalogName: string,
  @query entity_type?: string,
  @query schema_name?: string,
  @query table_name?: string,
  @query limit?: int32,
): Ok<CatalogHistoryResponse> | ResourceErrorResponses;

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
