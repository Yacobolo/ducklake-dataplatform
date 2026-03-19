using TypeSpec.Http;

namespace Duck;

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
): Ok<PaginatedGrants> | GuardedReadErrorResponses;

@route("/grants")
@post
@tag("Governance")
@summary("Create grant")
op createGrant(@body body: CreateGrantRequest): Created<PrivilegeGrant> | ValidationResourceErrorResponses;

@route("/grants/{grantId}")
@delete
@tag("Governance")
@summary("Delete grant")
op deleteGrant(grantId: string): NoContent | MutatingErrorResponses;

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

@route("/row-filters/{rowFilterId}/bindings/{principalType}/{principalId}")
@delete
@tag("Governance")
@summary("Unbind row filter")
op unbindRowFilter(rowFilterId: string, principalType: PrincipalType, principalId: string): NoContent | MutatingErrorResponses;

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

@route("/column-masks/{columnMaskId}/bindings/{principalType}/{principalId}")
@delete
@tag("Governance")
@summary("Unbind column mask")
op unbindColumnMask(columnMaskId: string, principalType: PrincipalType, principalId: string): NoContent | MutatingErrorResponses;

@route("/tags")
@get
@tag("Governance")
@summary("List tags")
op listTags(@query max_results?: int32, @query page_token?: string): Ok<PaginatedTags> | StandardErrorResponses;

@route("/tags")
@post
@tag("Governance")
@summary("Create tag")
op createTag(@body body: CreateTagRequest): Created<Tag> | MutatingErrorResponses;

@route("/tags/{tagId}")
@delete
@tag("Governance")
@summary("Delete tag")
op deleteTag(tagId: string): NoContent | MutatingErrorResponses;

@route("/tags/{tagId}/assignments")
@get
@tag("Governance")
@summary("List tag assignments")
op listTagAssignments(
  tagId: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedTagAssignments> | ResourceErrorResponses;

@route("/tags/{tagId}/assignments")
@post
@tag("Governance")
@summary("Create tag assignment")
op createTagAssignment(tagId: string, @body body: CreateTagAssignmentRequest): Created<TagAssignment> | ValidationResourceErrorResponses;

@route("/tags/{tagId}/assignments/{assignmentId}")
@delete
@tag("Governance")
@summary("Delete tag assignment")
op deleteTagAssignment(tagId: string, assignmentId: string): NoContent | MutatingErrorResponses;

@route("/classifications")
@get
@tag("Governance")
@summary("List classifications")
op listClassifications(@query max_results?: int32, @query page_token?: string): Ok<PaginatedTags> | StandardErrorResponses;
